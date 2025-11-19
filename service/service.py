"""Understanding/Converting 파이프라인을 오케스트레이션하는 서비스 레이어.

서비스 진입점에서 프로젝트 분석, DDL 처리, 후처리까지의 흐름을 다루며
각 단계에 상세한 docstring을 제공하여 운영자가 전체 과정을 쉽게 파악하도록 돕는다.
"""

import asyncio
import json
import logging
import shutil
import zipfile
import aiofiles
import os
from typing import Any, AsyncGenerator
from fastapi import HTTPException

from prompt.understand_ddl import understand_ddl
from prompt.understand_variables_prompt import resolve_table_variable_type
from prompt.understand_column_prompt import understand_column_roles
from understand.neo4j_connection import Neo4jConnection
from understand.analysis import Analyzer
from util.exception import FileProcessingError
from util.utility_tool import add_line_numbers, parse_table_identifier, emit_message, emit_data, emit_error, escape_for_cypher, parse_json_maybe
from util.llm_client import get_llm


# ----- 상수 -----
BASE_DIR = os.getenv('DOCKER_COMPOSE_CONTEXT') or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEST_SESSIONS = ("EN_TestSession", "KO_TestSession")
DDL_MAX_CONCURRENCY = int(os.getenv('DDL_MAX_CONCURRENCY', '5'))


# ----- 서비스 오케스트레이터 클래스 -----
class ServiceOrchestrator:
    """
    Understanding과 Converting 전체 프로세스를 관리하는 오케스트레이터 클래스
    """

    def __init__(self, user_id: str, api_key: str, locale: str, project_name: str, dbms: str, target_lang: str = 'java', update_mode: str = 'merge'):
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
        self.update_mode = update_mode if update_mode in ('skip', 'merge') else 'merge'
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
        """API 키가 유효한지 확인합니다.

        테스트 세션(EN/KO_TestSession)은 외부 호출 없이 통과시키고, 그 외에는
        간단한 ping 호출로 OpenAI 호환 API가 응답하는지 검증합니다.
        실패 시 HTTP 401 오류를 발생시켜 프론트엔드에서 바로 감지할 수 있도록 합니다.
        """
        if self.user_id in TEST_SESSIONS:
            return
        
        try:
            llm = get_llm(api_key=self.api_key)
            if not llm.invoke("ping"):
                raise HTTPException(status_code=401, detail="API 키 검증 실패: ping 실패")
        except Exception as e:
            logging.error(f"API 키 검증 실패: {str(e)}")
            raise HTTPException(status_code=401, detail=f"API 키 검증 실패: {e.__class__.__name__}: {str(e)}")

    # ----- Understanding 프로세스 -----

    async def understand_project(self, file_names: list) -> AsyncGenerator[bytes, None]:
        """PL/SQL 파일 묶음을 분석하고 Neo4j 그래프 이벤트를 스트리밍합니다.

        Args:
            file_names: `(folder_name, file_name)` 형식의 튜플 리스트

        Yields:
            bytes: 프론트엔드로 전송하는 스트리밍 이벤트(JSON 직렬화 결과)
        """
        connection = Neo4jConnection()
        events_from_analyzer = asyncio.Queue()
        events_to_analyzer = asyncio.Queue()

        try:
            yield emit_message("Preparing Analysis Data")

            # 병합 모드 안전성 보장: 유니크 제약 생성(멱등)
            await connection.ensure_constraints()

            # 이미 분석된 경우: skip 또는 merge 모드 분기
            if await connection.node_exists(self.user_id, file_names):
                if self.update_mode == 'skip':
                    yield emit_message("ALREADY ANALYZED")
                    graph_data = await connection.execute_query_and_return_graph(self.user_id, file_names)
                    yield emit_data(graph=graph_data, analysis_progress=100)
                    return
                else:
                    yield emit_message("ALREADY ANALYZED: MERGE MODE - RE-APPLYING UPDATES")

            # DDL 파일 처리
            ddl_files = self._list_ddl_files()
            if ddl_files:
                ddl_dir = self.dirs['ddl']
                ddl_semaphore = asyncio.Semaphore(DDL_MAX_CONCURRENCY)
                ddl_tasks = []

                async def _run_single_ddl(file_name: str):
                    async with ddl_semaphore:
                        ddl_file_path = os.path.join(ddl_dir, file_name)
                        await self._process_ddl(ddl_file_path, connection, file_name)

                for ddl_file_name in ddl_files:
                    yield emit_message(f"START DDL PROCESSING: {ddl_file_name}")
                    logging.info(f"DDL 파일 처리 시작: {ddl_file_name}")
                    ddl_tasks.append(asyncio.create_task(_run_single_ddl(ddl_file_name)))

                if ddl_tasks:
                    await asyncio.gather(*ddl_tasks)

            # PL/SQL 파일 분석
            for folder_name, file_name in file_names:
                await self._ensure_folder_node(connection, folder_name)
                async for chunk in self._analyze_file(
                    folder_name, file_name, file_names, connection,
                    events_from_analyzer, events_to_analyzer
                ):
                    yield chunk

            yield emit_message("ALL_ANALYSIS_COMPLETED")
        finally:
            await connection.close()

    async def _analyze_file(self, folder_name: str, file_name: str, file_pairs: list,
                           connection: Neo4jConnection, events_from_analyzer: asyncio.Queue,
                           events_to_analyzer: asyncio.Queue) -> AsyncGenerator[bytes, None]:
        """단일 PL/SQL 파일에 대한 Analyzer 실행과 이벤트 스트리밍을 담당합니다."""
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
                yield emit_data(graph=graph_result, line_number=last_line, analysis_progress=100, current_file=current_file)
                break

            if result_type == 'error':
                logging.info(f"Understanding Failed for {file_name}")
                break

            # 중간 진행 상황 전송
            next_analysis_line = analysis_result['line_number']
            await connection.execute_queries(analysis_result.get('query_data', []))
            graph_result = await connection.execute_query_and_return_graph(self.user_id, file_pairs)
            yield emit_data(graph=graph_result, line_number=next_analysis_line, analysis_progress=int((next_analysis_line / last_line) * 100), current_file=current_file)
            await events_to_analyzer.put({'type': 'process_completed'})

        await analysis_task

    async def _postprocess_file(self, connection: Neo4jConnection, folder_name: str, 
                                file_name: str, file_pairs: list) -> None:
        """분석 완료 후 변수 타입 보정과 컬럼 역할 요약을 적용합니다."""
        folder_esc, file_esc = escape_for_cypher(folder_name), escape_for_cypher(file_name)
        
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
                    parse_json_maybe(row.get('columns')),
                    self.api_key, self.locale
                )
                for row in var_rows
            ])
            
            user_id_esc = escape_for_cypher(self.user_id)
            update_queries = [
                f"MATCH (v:Variable {{name: '{escape_for_cypher(row['varName'])}', folder_name: '{folder_esc}', file_name: '{file_esc}', user_id: '{user_id_esc}'}}) "
                f"SET v.type = '{escape_for_cypher((result or {}).get('resolvedType') or row.get('declaredType'))}', v.resolved = true"
                for row, result in zip(var_rows, type_results)
            ]

            if update_queries:
                await connection.execute_queries(update_queries)

    async def _process_ddl(self, ddl_file_path: str, connection: Neo4jConnection, file_name: str) -> None:
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
                # 폴더 기준 유니크 키 사용을 위해 DDL 경로에서는 folder_name='system'으로 통일
                t_merge_key = {**common_props, 'folder_name': 'system', 'schema': effective_schema, 'name': parsed_table}
                t_merge_str = ', '.join(f"`{k}`: '{v}'" for k, v in t_merge_key.items())
                # detailDescription 초기 구성 (사람이 읽는 텍스트로 저장)
                lines = []
                summary_line = f"설명: {table_comment}" if table_comment else "설명: "
                lines.append(summary_line)
                lines.append("")
                lines.append("주요  컬럼:")
                for col in columns:
                    col_name_i = (col.get('name') or '').strip()
                    if not col_name_i:
                        continue
                    role = (col.get('comment') or '').strip()
                    # DDL 단계에서는 예시값이 없으므로 예시는 생략
                    lines.append(f"   {col_name_i}: {role}" if role else f"   {col_name_i}: ")
                detail_desc_text = "\n".join(lines)

                t_set_props = {**common_props, 'description': escape_for_cypher(table_comment), 'table_type': table_type, 'detailDescription': escape_for_cypher(detail_desc_text)}
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

                    c_merge_key = {'user_id': self.user_id, 'fqn': fqn, 'project_name': self.project_name}
                    c_merge_str = ', '.join(f"`{k}`: '{v}'" for k, v in c_merge_key.items())
                    c_set_props = {
                        'name': escape_for_cypher(col_name),
                        'dtype': escape_for_cypher(col_type),
                        'description': escape_for_cypher(col_comment),
                        'nullable': 'true' if col_nullable else 'false',
                        'project_name': self.project_name,
                        'fqn': fqn
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
            logging.info(f"DDL 파일 처리 완료: {file_name}")

    async def _ensure_folder_node(self, connection: Neo4jConnection, folder_name: str) -> None:
        """폴더 이름에 대응하는 SYSTEM 노드를 생성하여 그래프 루트를 보장합니다."""
        user_id_esc, folder_esc, project_esc = escape_for_cypher(self.user_id), escape_for_cypher(folder_name), escape_for_cypher(self.project_name)
        await connection.execute_queries([
            f"MERGE (f:SYSTEM {{user_id: '{user_id_esc}', name: '{folder_esc}', project_name: '{project_esc}', has_children: true}}) RETURN f"
        ])

    async def _load_assets(self, folder_name: str, file_name: str) -> tuple:
        """분석에 필요한 ANTLR JSON 및 원본 PL/SQL 텍스트를 동시에 로드합니다."""
        folder_dir = os.path.join(self.dirs['plsql'], folder_name)
        plsql_file_path = os.path.join(folder_dir, file_name)
        base_name = os.path.splitext(file_name)[0]
        analysis_file_path = os.path.join(self.dirs['analysis'], folder_name, f"{base_name}.json")

        async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as antlr_file, \
                 aiofiles.open(plsql_file_path, 'r', encoding='utf-8') as plsql_file:
            antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())
            return json.loads(antlr_data), plsql_content

    def _list_ddl_files(self) -> list:
        """DDL 디렉터리에서 처리 대상 파일 목록을 반환합니다."""
        try:
            ddl_dir = self.dirs['ddl']
            return [f for f in sorted(os.listdir(ddl_dir)) if os.path.isfile(os.path.join(ddl_dir, f))]
        except Exception:
            return []

    # ----- Converting 프로세스 -----

    async def convert_project(
        self,
        file_names: list,
        conversion_type: str = 'framework',
        target_framework: str = 'springboot',
        target_dbms: str = 'oracle'
    ) -> AsyncGenerator[bytes, None]:
        """변환 타입에 따라 적절한 전략을 선택하여 변환을 수행합니다.
        
        Args:
            file_names: 변환할 파일 목록 [(folder_name, file_name), ...]
            conversion_type: 변환 타입 ('framework' 또는 'dbms')
            target_framework: 타겟 프레임워크 (기본값: 'springboot')
            target_dbms: 타겟 DBMS (기본값: 'oracle')
            
        Yields:
            bytes: 스트리밍 응답 데이터
        """
        from convert.strategies.strategy_factory import StrategyFactory
        
        logging.info("Convert: type=%s, project=%s, files=%d, target=%s",
                    conversion_type, self.project_name, len(file_names),
                    target_framework if conversion_type == 'framework' else f"{self.dbms}→{target_dbms}")

        # 전략 생성
        strategy = StrategyFactory.create_strategy(
            conversion_type,
            target_dbms=target_dbms,
            target_framework=target_framework
        )

        # 전략 실행
        async for chunk in strategy.convert(file_names, orchestrator=self):
            yield chunk

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