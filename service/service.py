import asyncio
import json
import logging
import shutil
import zipfile
import aiofiles
import os
from typing import Any, AsyncGenerator
from fastapi import HTTPException

from convert.create_controller import ControllerGenerator, start_controller_skeleton_processing
from convert.create_config_files import ConfigFilesGenerator
from convert.create_repository import RepositoryGenerator
from convert.create_entity import EntityGenerator
from convert.create_service_preprocessing import start_service_preprocessing
from convert.create_service_skeleton import ServiceSkeletonGenerator
from convert.create_main import MainClassGenerator
from prompt.understand_ddl import understand_ddl
from prompt.understand_variables_prompt import resolve_table_variable_type
from prompt.understand_column_prompt import understand_column_roles
from understand.neo4j_connection import Neo4jConnection
from understand.analysis import Analyzer
from util.exception import ConvertingError, FileProcessingError
from util.utility_tool import add_line_numbers, parse_table_identifier
from util.llm_client import get_llm


# ----- 상수 -----
BASE_DIR = os.getenv('DOCKER_COMPOSE_CONTEXT') or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEST_SESSIONS = ("EN_TestSession", "KO_TestSession")
STREAM_DELIMITER = b"send_stream"


# ----- 서비스 오케스트레이터 클래스 -----
class ServiceOrchestrator:
    """
    Understanding과 Converting 전체 프로세스를 관리하는 오케스트레이터 클래스
    """

    def __init__(self, user_id: str, api_key: str, locale: str, project_name: str, dbms: str, target_lang: str = 'java'):
        """
        ServiceOrchestrator 초기화
        
        Args:
            user_id: 사용자 식별자
            api_key: LLM API 키
            locale: 언어 설정
            project_name: 프로젝트 이름
            dbms: 데이터베이스 종류
            target_lang: 타겟 언어 (기본값: 'java')
        """
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name
        self.dbms = dbms
        self.target_lang = target_lang
        self.project_name_cap = project_name.capitalize() if project_name else ''
        
        # 디렉토리 경로 설정
        if project_name:
            user_base = os.path.join(BASE_DIR, 'data', user_id, project_name)
            self.dirs = {
                'plsql': os.path.join(user_base, "src"),
                'analysis': os.path.join(user_base, "analysis"),
                'ddl': os.path.join(user_base, "ddl"),
            }

    # ----- API 키 검증 -----

    async def validate_api_key(self) -> None:
        """API 키 유효성 검증 (테스트 세션은 스킵)"""
        if self.user_id in TEST_SESSIONS:
            return
        
        try:
            llm = get_llm(api_key=self.api_key)
            if not llm.invoke("ping"):
                raise HTTPException(status_code=401, detail="유효하지 않은 API 키입니다.")
        except Exception as e:
            logging.error(f"API 키 검증 실패: {str(e)}")
            raise HTTPException(status_code=401, detail="유효하지 않은 API 키입니다.")

    # ----- Understanding 프로세스 -----

    async def understand_project(self, file_names: list) -> AsyncGenerator[bytes, None]:
        """
        PL/SQL 파일 분석 및 Neo4j 그래프 데이터 생성
        
        Args:
            file_names: [(folder_name, file_name), ...] 리스트
        
        Yields:
            bytes: 스트리밍 응답 데이터
        """
        connection = Neo4jConnection()
        events_from_analyzer = asyncio.Queue()
        events_to_analyzer = asyncio.Queue()

        try:
            yield self._stream_alarm("Preparing Analysis Data")

            # 이미 분석된 경우
            if await connection.node_exists(self.user_id, file_names):
                yield self._stream_alarm("ALREADY ANALYZED")
                graph_data = await connection.execute_query_and_return_graph(self.user_id, file_names)
                yield self._stream_data(graph=graph_data, analysis_progress=100)
                return

            # DDL 파일 처리
            ddl_files = self._list_ddl_files()
            if ddl_files:
                ddl_dir = self.dirs['ddl']
                for ddl_file_name in ddl_files:
                    yield self._stream_alarm("START DDL PROCESSING", file=ddl_file_name)
                    logging.info(f"DDL 파일 처리 시작: {ddl_file_name}")
                    
                    try:
                        ddl_file_path = os.path.join(ddl_dir, ddl_file_name)
                        base_object_name = os.path.splitext(ddl_file_name)[0]
                        await self._process_ddl(ddl_file_path, connection, base_object_name)
                    except Exception as e:
                        logging.error(f"DDL 파일 처리 실패: {ddl_file_name} - {e}")
                        continue

            # PL/SQL 파일 분석
            for folder_name, file_name in file_names:
                await self._ensure_folder_node(connection, folder_name)
                async for chunk in self._analyze_file(
                    folder_name, file_name, file_names, connection, 
                    events_from_analyzer, events_to_analyzer
                ):
                    yield chunk

            yield self._stream_alarm("ALL_ANALYSIS_COMPLETED")

        except Exception as e:
            logging.exception(f"Understanding 처리 중 오류: {str(e)}")
            yield self._stream_error(str(e))
        finally:
            await connection.close()

    async def _analyze_file(self, folder_name: str, file_name: str, file_pairs: list,
                           connection: Neo4jConnection, events_from_analyzer: asyncio.Queue,
                           events_to_analyzer: asyncio.Queue) -> AsyncGenerator[bytes, None]:
        """단일 파일 분석 처리"""
        # ANTLR 데이터 및 PL/SQL 내용 로드
        antlr_data, plsql_content = await self._load_assets(folder_name, file_name)
        last_line = len(plsql_content)
        plsql_numbered, _ = add_line_numbers(plsql_content)

        # Analyzer 실행
        analyzer = Analyzer(
            antlr_data=antlr_data,
            file_content=plsql_numbered,
            send_queue=events_from_analyzer,
            receive_queue=events_to_analyzer,
            last_line=last_line,
            folder_name=folder_name,
            file_name=file_name,
            user_id=self.user_id,
            api_key=self.api_key,
            locale=self.locale,
            dbms=self.dbms,
            project_name=self.project_name,
        )
        analysis_task = asyncio.create_task(analyzer.run())

        # 분석 이벤트 처리
        current_file = f"{folder_name}-{file_name}"
        while True:
            analysis_result = await events_from_analyzer.get()
            result_type = analysis_result.get('type')
            
            logging.info(f"Analysis Event: {current_file}")

            if result_type == 'end_analysis':
                logging.info(f"Understanding Completed for {current_file}\n")
                await self._postprocess_file(connection, folder_name, file_name, file_pairs)
                graph_result = await connection.execute_query_and_return_graph(self.user_id, file_pairs)
                yield self._stream_data(graph=graph_result, line_number=last_line, analysis_progress=100, current_file=current_file)
                break

            if result_type == 'error':
                logging.info(f"Understanding Failed for {file_name}")
                break

            # 중간 진행 상황 전송
            next_analysis_line = analysis_result['line_number']
            await connection.execute_queries(analysis_result.get('query_data', []))
            graph_result = await connection.execute_query_and_return_graph(self.user_id, file_pairs)
            yield self._stream_data(graph=graph_result, line_number=next_analysis_line, analysis_progress=int((next_analysis_line / last_line) * 100), current_file=current_file)
            await events_to_analyzer.put({'type': 'process_completed'})

        await analysis_task

    async def _postprocess_file(self, connection: Neo4jConnection, folder_name: str, 
                                file_name: str, file_pairs: list) -> None:
        """파일 분석 후처리: 변수 타입 해석 및 컬럼 역할 산출"""
        folder_esc, file_esc = self._escape(folder_name), self._escape(file_name)
        
        # 변수 타입 해석
        var_rows = (await connection.execute_queries([f"""
            MATCH (v:Variable {{folder_name: '{folder_esc}', file_name: '{file_esc}', user_id: '{self.user_id}'}})
            WITH v,
                trim(replace(replace(coalesce(v.value, ''), 'Table: ', ''), 'Table:', '')) AS valueAfterPrefix,
                coalesce(v.type, '') AS vtype
            WITH v, trim(replace(CASE WHEN vtype <> '' THEN vtype ELSE valueAfterPrefix END, ' ', '')) AS raw
            WITH v,
                CASE WHEN raw CONTAINS '.' THEN split(raw, '.')[0] ELSE '' END AS schemaName,
                CASE WHEN raw CONTAINS '.' THEN split(raw, '.')[1] ELSE raw END AS tableName
            MATCH (t:Table {{user_id: '{self.user_id}', name: toUpper(tableName)}})
            WHERE coalesce(t.schema, '') = coalesce(toUpper(schemaName), '')
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{self.user_id}'}})
            WITH v, coalesce(toUpper(schemaName), '') AS schema, toUpper(tableName) AS table,
                collect(DISTINCT {{name: c.name, dtype: coalesce(c.dtype, ''), nullable: toBoolean(c.nullable), comment: coalesce(c.description, '')}}) AS columns
            RETURN v.name AS varName, v.type AS declaredType, schema, table, columns
        """]))[0] if connection else []

        if var_rows:
            # 딕셔너리 접근 최적화 및 JSON 파싱 최적화
            type_results = await asyncio.gather(*[
                resolve_table_variable_type(
                    row['varName'], row.get('declaredType'), row['schema'], row['table'],
                    self._parse_json(row.get('columns')),
                    self.api_key, self.locale
                )
                for row in var_rows
            ])
            
            user_id_esc = self._escape(self.user_id)
            update_queries = [
                f"MATCH (v:Variable {{name: '{self._escape(row['varName'])}', folder_name: '{folder_esc}', file_name: '{file_esc}', user_id: '{user_id_esc}'}}) "
                f"SET v.type = '{self._escape((result or {}).get('resolvedType') or row.get('declaredType'))}', v.resolved = true"
                for row, result in zip(var_rows, type_results)
            ]

            if update_queries:
                await connection.execute_queries(update_queries)

        # 컬럼 역할 산출
        table_rows = (await connection.execute_queries([f"""
            MATCH (folder:Folder {{user_id: '{self.user_id}', name: '{folder_esc}'}})-[:CONTAINS]->(t:Table)
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{self.user_id}'}})
            OPTIONAL MATCH (dml)-[:FROM|WRITES]->(t)
            WITH t, collect(DISTINCT dml.summary) AS dmlSummaries,
                 collect(DISTINCT {{name: c.name, dtype: coalesce(c.dtype, ''), nullable: toBoolean(c.nullable), comment: coalesce(c.description, '')}}) AS columns
            RETURN coalesce(t.schema,'') AS schema, t.name AS table, columns, dmlSummaries
        """]))[0] if connection else []

        if table_rows:
            roles_results = await asyncio.gather(*[
                understand_column_roles(
                    self._parse_json(row.get('columns')),
                    self._parse_json(row.get('dmlSummaries')),
                    self.api_key, self.locale
                )
                for row in table_rows
            ], return_exceptions=True)
            
            column_update_queries = []
            user_id_esc = self._escape(self.user_id)
            for row, roles_result in zip(table_rows, roles_results):
                if isinstance(roles_result, Exception):
                    continue
                
                schema, table = row.get('schema', ''), row['table']
                match_table = f"MATCH (t:Table {{user_id: '{user_id_esc}', name: '{table}'}}) WHERE coalesce(t.schema,'') = '{schema}'"
                roles_dict = roles_result or {}
                
                # 테이블 설명 업데이트
                if table_desc := roles_dict.get('tableDescription'):
                    column_update_queries.append(f"{match_table} SET t.description = '{self._escape(table_desc)}'")
                
                # 컬럼 역할 업데이트
                for role_item in (roles_dict.get('roles') or []):
                    if col_name := role_item.get('name'):
                        column_update_queries.append(
                            f"{match_table} "
                            f"MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{user_id_esc}', name: '{self._escape(col_name)}'}}) "
                            f"SET c.description = '{self._escape(role_item.get('role', ''))}'"
                        )

            if column_update_queries:
                await connection.execute_queries(column_update_queries)

    async def _process_ddl(self, ddl_file_path: str, connection: Neo4jConnection, object_name: str) -> None:
        """DDL 파일 처리하여 Table/Column 노드 생성"""
        async with aiofiles.open(ddl_file_path, 'r', encoding='utf-8') as ddl_file:
            ddl_content = await ddl_file.read()
            parsed = understand_ddl(ddl_content, self.api_key, self.locale)
            cypher_queries = []
            
            # 공통 속성 캐싱
            common_props = {'user_id': self.user_id, 'db': self.dbms, 'project_name': self.project_name}

            for table in parsed['analysis']:
                table_info = table['table']
                columns = table.get('columns', [])
                foreign_list = table.get('foreignKeys', [])
                primary_list = [s for pk in (table.get('primaryKeys') or []) if (s := str(pk).strip().upper())]

                # Table 식별 (한번에 추출)
                orig_schema, orig_table, table_comment, table_type = (
                    (table_info.get('schema') or '').strip(),
                    (table_info.get('name') or '').strip(),
                    (table_info.get('comment') or '').strip(),
                    (table_info.get('table_type') or 'BASE TABLE').strip().upper()
                )
                qualified_table = f"{orig_schema}.{orig_table}" if orig_schema else orig_table
                parsed_schema, parsed_table, _ = parse_table_identifier(qualified_table)
                effective_schema = parsed_schema or ''

                # Table 노드 MERGE
                t_merge_key = {**common_props, 'schema': effective_schema, 'name': parsed_table}
                t_merge_str = ', '.join(f"`{k}`: '{v}'" for k, v in t_merge_key.items())
                t_set_props = {**common_props, 'description': self._escape(table_comment), 'table_type': table_type}
                t_set_str = ', '.join(f"t.`{k}` = '{v}'" for k, v in t_set_props.items())
                cypher_queries.append(f"MERGE (t:Table {{{t_merge_str}}}) SET {t_set_str}")

                # Column 노드 MERGE
                for col in columns:
                    if not (col_name := (col.get('name') or '').strip()):
                        continue
                    
                    col_type = (col.get('dtype') or col.get('type') or '').strip()
                    col_nullable = col.get('nullable', True)
                    col_comment = (col.get('comment') or '').strip()
                    fqn = '.'.join(filter(None, [effective_schema, parsed_table, col_name])).lower()

                    c_merge_key = {'user_id': self.user_id, 'name': col_name, 'fqn': fqn, 'project_name': self.project_name}
                    c_merge_str = ', '.join(f"`{k}`: '{v}'" for k, v in c_merge_key.items())
                    c_set_props = {
                        'dtype': self._escape(col_type),
                        'description': self._escape(col_comment),
                        'nullable': 'true' if col_nullable else 'false',
                        'project_name': self.project_name
                    }
                    if col_name.upper() in primary_list:
                        c_set_props['pk_constraint'] = f"{parsed_table}_pkey"
                    
                    c_set_str = ', '.join(f"c.`{k}` = '{v}'" for k, v in c_set_props.items())
                    cypher_queries.append(f"MERGE (c:Column {{{c_merge_str}}}) SET {c_set_str}")
                    cypher_queries.append(f"MATCH (t:Table {{{t_merge_str}}})\nMATCH (c:Column {{{c_merge_str}}})\nMERGE (t)-[:HAS_COLUMN]->(c)")

                # FK 관계 구성
                for fk in foreign_list:
                    src_col = (fk.get('column') or '').strip()
                    ref = (fk.get('ref') or '').strip()
                    if not src_col or not ref or '.' not in ref:
                        continue
                    
                    table_qualifier, ref_column = ref.rsplit('.', 1)
                    ref_schema, ref_table, _ = parse_table_identifier(table_qualifier)
                    ref_schema = ref_schema or effective_schema

                    ref_table_merge_key = {**common_props, 'schema': ref_schema or '', 'name': ref_table or ''}
                    ref_table_merge_str = ', '.join(f"`{k}`: '{v}'" for k, v in ref_table_merge_key.items())
                    cypher_queries.append(f"MERGE (rt:Table {{{ref_table_merge_str}}})")
                    cypher_queries.append(f"MATCH (t:Table {{{t_merge_str}}})\nMATCH (rt:Table {{{ref_table_merge_str}}})\nMERGE (t)-[:FK_TO_TABLE]->(rt)")

                    src_fqn = '.'.join(filter(None, [effective_schema, parsed_table, src_col])).lower()
                    ref_fqn = '.'.join(filter(None, [ref_schema or effective_schema, ref_table, ref_column])).lower()

                    src_c_key = {'user_id': self.user_id, 'name': src_col, 'fqn': src_fqn, 'project_name': self.project_name}
                    ref_c_key = {'user_id': self.user_id, 'name': ref_column, 'fqn': ref_fqn, 'project_name': self.project_name}
                    src_c_str = ', '.join(f"`{k}`: '{v}'" for k, v in src_c_key.items())
                    ref_c_str = ', '.join(f"`{k}`: '{v}'" for k, v in ref_c_key.items())
                    
                    cypher_queries.append(f"MERGE (sc:Column {{{src_c_str}}})")
                    cypher_queries.append(f"MERGE (dc:Column {{{ref_c_str}}})")
                    cypher_queries.append(f"MATCH (sc:Column {{{src_c_str}}})\nMATCH (dc:Column {{{ref_c_str}}})\nMERGE (sc)-[:FK_TO]->(dc)")

            await connection.execute_queries(cypher_queries)
            logging.info(f"DDL 파일 처리 완료: {object_name}")

    async def _ensure_folder_node(self, connection: Neo4jConnection, folder_name: str) -> None:
        """폴더 노드 생성"""
        user_id_esc, folder_esc, project_esc = self._escape(self.user_id), self._escape(folder_name), self._escape(self.project_name)
        await connection.execute_queries([
            f"MERGE (f:Folder {{user_id: '{user_id_esc}', name: '{folder_esc}', project_name: '{project_esc}', has_children: true}}) RETURN f"
        ])

    async def _load_assets(self, folder_name: str, file_name: str) -> tuple:
        """ANTLR 분석 JSON과 PL/SQL 파일 로드"""
        folder_dir = os.path.join(self.dirs['plsql'], folder_name)
        plsql_file_path = os.path.join(folder_dir, file_name)
        base_name = os.path.splitext(file_name)[0]
        analysis_file_path = os.path.join(self.dirs['analysis'], folder_name, f"{base_name}.json")

        async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as antlr_file, \
                 aiofiles.open(plsql_file_path, 'r', encoding='utf-8') as plsql_file:
            antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())
            return json.loads(antlr_data), plsql_content

    def _list_ddl_files(self) -> list:
        """DDL 디렉토리 파일 목록 조회"""
        try:
            ddl_dir = self.dirs['ddl']
            return [f for f in sorted(os.listdir(ddl_dir)) if os.path.isfile(os.path.join(ddl_dir, f))]
        except Exception:
            return []

    # ----- Converting 프로세스 -----

    async def convert_to_springboot(self, file_names: list) -> AsyncGenerator[bytes, None]:
        """
        Spring Boot 프로젝트 생성
        
        Args:
            file_names: [(folder_name, file_name), ...] 리스트
        
        Yields:
            bytes: 스트리밍 응답 데이터
        """
        try:
            yield self._emit("data", file_type="project_name", project_name=self.project_name)
            file_count = len(file_names)
            
            # Generator 공통 파라미터
            gen_params = (self.project_name, self.user_id, self.api_key, self.locale, self.target_lang)

            for current_index, (folder_name, file_name) in enumerate(file_names, start=1):
                base_name = os.path.splitext(file_name)[0]
                
                # Step 1: Entity
                yield self._emit("message", step=1, content=f"{base_name} - Generating Entity Class")
                entity_result_list = await EntityGenerator(*gen_params).generate()
                for entity in entity_result_list:
                    entity_name, entity_code = entity['entityName'], entity['entityCode']
                    yield self._emit("data", file_type="entity_class", file_name=f"{entity_name}.java", code=entity_code)
                yield self._emit("Done", step=1, file_count=file_count, current_count=current_index)

                # Step 2: Repository
                yield self._emit("message", step=2, content=f"{base_name} - Generating Repository Interface")
                used_query_methods, global_variables, sequence_methods, repository_list = await RepositoryGenerator(*gen_params).generate()
                for repo in repository_list:
                    repo_name, repo_code = repo['repositoryName'], repo['code']
                    yield self._emit("data", file_type="repository_class", file_name=f"{repo_name}.java", code=repo_code)
                yield self._emit("Done", step=2, file_count=file_count, current_count=current_index)

                # Step 3: Service Skeleton - service class 틀 생성
                yield self._emit("message", step=3, content="Business Logic Processing")
                logging.info(f"Start converting {base_name}\n")

                yield self._emit("message", step=3, content=f"{base_name} - Service Skeleton")
                service_creation_info, service_class_name, exist_command_class, command_class_list = (
                    await ServiceSkeletonGenerator(*gen_params).generate(entity_result_list, folder_name, file_name, global_variables)
                )

                # Step 3: Service Skeleton - command class 생성
                yield self._emit("message", step=3, content=f"{base_name} - Command Class")
                for command in command_class_list:
                    cmd_name, cmd_code = command['commandName'], command['commandCode']
                    yield self._emit("data", file_type="command_class", file_name=f"{cmd_name}.java", code=cmd_code)
                yield self._emit("Done", step=3, file_count=file_count, current_count=current_index)

                # Step 4: Service 바디 & Controller 생성
                yield self._emit("message", step=4, content=f"{base_name} - Service Controller Processing")
                
                # Service 생성
                for svc in service_creation_info:
                    svc_skeleton, cmd_var, proc_name = (
                        svc['service_method_skeleton'], svc['command_class_variable'], svc['procedure_name']
                    )
                    
                    # Service 바디 생성
                    service_code = await start_service_preprocessing(
                        svc_skeleton, cmd_var, proc_name,
                        used_query_methods, folder_name, file_name, sequence_methods,
                        self.project_name,
                        self.user_id, self.api_key, self.locale,
                        self.target_lang
                    )
                    
                    # Service 파일 스트리밍 이벤트 전송
                    yield self._emit("data", file_type="service_class", file_name=f"{service_class_name}.java", code=service_code)
                
                # Controller 생성 
                controller_name, controller_code = await ControllerGenerator(*gen_params).generate(
                    base_name, service_class_name, exist_command_class, service_creation_info
                )
                yield self._emit("data", file_type="controller_class", file_name=f"{controller_name}.java", code=controller_code)
                
                yield self._emit("message", step=4, content=f"{base_name} - Service & Controller 생성 완료")
                yield self._emit("Done", step=4)

            # Step 5: Config Files + Main Class
            yield self._emit("message", step=5, content="Generating Configuration Files")
            
            # Config Files 생성
            pom_xml_code, properties_code = await ConfigFilesGenerator(self.project_name, self.user_id).generate()
            yield self._emit("data", file_type="pom", file_name="pom.xml", code=pom_xml_code)
            yield self._emit("data", file_type="properties", file_name="application.properties", code=properties_code)

            # Main Class 생성
            main_code = await MainClassGenerator(self.project_name, self.user_id).generate()
            yield self._emit("message", step=5, content="Generating Main Application")
            yield self._emit("data", file_type="main", file_name=f"{self.project_name_cap}Application.java", code=main_code)
            yield self._emit("Done", step=5)

        except ConvertingError:
            raise
        except Exception as e:
            logging.error(f"스프링 부트 프로젝트 변환 중 오류: {str(e)}")
            raise ConvertingError(f"스프링 부트 프로젝트 변환 중 오류: {str(e)}")

    # ----- 파일 작업 -----

    async def zip_project(self, source_directory: str, output_zip_path: str) -> None:
        """프로젝트 디렉토리를 ZIP으로 압축"""
        try:
            os.makedirs(os.path.dirname(output_zip_path), exist_ok=True)
            logging.info(f"Zipping {source_directory} to {output_zip_path}")
            
            with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(source_directory):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zipf.write(file_path, os.path.relpath(file_path, source_directory))
            
            logging.info("Zipping completed successfully.")
        except Exception as e:
            logging.error(f"Zip 압축 중 오류: {str(e)}")
            raise FileProcessingError(f"Zip 압축 중 오류: {str(e)}")

    async def cleanup_all_data(self) -> None:
        """사용자 데이터 전체 삭제 (파일 + Neo4j)"""
        connection = Neo4jConnection()
        
        try:
            # 파일 삭제
            user_dirs = [
                os.path.join(BASE_DIR, 'data', self.user_id),
                os.path.join(BASE_DIR, 'target', 'java', self.user_id)
            ]
            
            for dir_path in user_dirs:
                if os.path.exists(dir_path):
                    shutil.rmtree(dir_path)
                    os.makedirs(dir_path)
                    logging.info(f"디렉토리 재생성 완료: {dir_path}")
            
            # Neo4j 데이터 삭제
            await connection.execute_queries([f"MATCH (n {{user_id: '{self.user_id}'}}) DETACH DELETE n"])
            logging.info(f"Neo4J 데이터 초기화 완료 - User ID: {self.user_id}")
        
        except Exception as e:
            logging.error(f"데이터 삭제 중 오류: {str(e)}")
            raise FileProcessingError(f"데이터 삭제 중 오류: {str(e)}")
        finally:
            await connection.close()

    # ----- 유틸리티 헬퍼 -----

    @staticmethod
    def _escape(text: str) -> str:
        """Cypher 쿼리용 문자열 이스케이프"""
        return str(text).replace("'", "\\'")

    @staticmethod
    def _parse_json(data):
        """JSON 문자열을 파싱하거나 리스트 그대로 반환"""
        if isinstance(data, str):
            return json.loads(data)
        return data or []

    # ----- 스트리밍 헬퍼 -----

    @staticmethod
    def _stream_bytes(payload: dict) -> bytes:
        """스트림 전송용 바이트 생성"""
        return json.dumps(payload, default=str).encode('utf-8') + STREAM_DELIMITER

    @staticmethod
    def _stream_alarm(message: str, **extra) -> bytes:
        """ALARM 타입 스트림 메시지"""
        return json.dumps({"type": "ALARM", "MESSAGE": message, **extra}, default=str).encode('utf-8') + STREAM_DELIMITER

    @staticmethod
    def _stream_data(**fields) -> bytes:
        """DATA 타입 스트림 메시지"""
        payload = {"type": "DATA"}
        payload.update({k: v for k, v in fields.items() if v is not None})
        return json.dumps(payload, default=str).encode('utf-8') + STREAM_DELIMITER

    @staticmethod
    def _stream_error(error_msg: str) -> bytes:
        """ERROR 타입 스트림 메시지"""
        return json.dumps({"error": error_msg}, default=str).encode('utf-8') + STREAM_DELIMITER

    @staticmethod
    def _emit(data_type: str, **kwargs) -> bytes:
        """Converting용 emit"""
        return json.dumps({"data_type": data_type, **kwargs}).encode('utf-8') + STREAM_DELIMITER
