import asyncio
import json
import logging
import shutil
from typing import Any, AsyncGenerator
import zipfile
import aiofiles
import os
import textwrap

#-------------------------------------------------------------------------#
# Imports
#-------------------------------------------------------------------------#
from convert.create_controller import generate_controller_class, start_controller_processing
from convert.create_controller_skeleton import start_controller_skeleton_processing
from convert.create_pomxml import start_pomxml_processing
from convert.create_properties import start_APLproperties_processing
from convert.create_repository import start_repository_processing
from convert.create_entity import start_entity_processing
from convert.create_service_preprocessing import start_service_preprocessing
from convert.create_service_postprocessing import generate_service_class, start_service_postprocessing
from convert.create_service_skeleton import start_service_skeleton_processing
from convert.create_main import start_main_processing
from prompt.convert_project_name_prompt import generate_project_name_prompt
from prompt.understand_ddl import understand_ddl
from prompt.understand_variables_prompt import resolve_table_variable_type
from prompt.understand_column_prompt import understand_column_roles
from understand.neo4j_connection import Neo4jConnection
from understand.analysis import Analyzer
from util.exception import ConvertingError, Neo4jError, UnderstandingError, FileProcessingError
from util.utility_tool import add_line_numbers
from util.llm_client import get_llm


#-------------------------------------------------------------------------#
# Constants & Base Paths
#-------------------------------------------------------------------------#
if os.getenv('DOCKER_COMPOSE_CONTEXT'):
    BASE_DIR = os.getenv('DOCKER_COMPOSE_CONTEXT')
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


#-------------------------------------------------------------------------#
# Path Utilities
#-------------------------------------------------------------------------#
def get_user_directories(user_id: str):
    """사용자 ID를 기준으로 작업 디렉터리 경로 딕셔너리를 반환합니다.

    매개변수:
    - user_id: 사용자 ID

    반환값:
    - 'plsql', 'analysis', 'ddl' 키를 갖는 디렉터리 경로 딕셔너리
    """
    user_base = os.path.join(BASE_DIR, 'data', user_id)
    return {
        'plsql': os.path.join(user_base, "src"),
        'analysis': os.path.join(user_base, "analysis"),
        'ddl': os.path.join(user_base, "ddl"),
    }


#-------------------------------------------------------------------------#
# Streaming Helpers
#-------------------------------------------------------------------------#
def _stream_bytes(payload: dict) -> bytes:
    """스트림 전송용 바이트를 생성합니다."""
    return json.dumps(payload).encode('utf-8') + b"send_stream"


def _alarm(message: str, **extra) -> bytes:
    """ALARM 타입 스트림 메시지를 생성합니다."""
    return _stream_bytes({"type": "ALARM", "MESSAGE": message, **extra})


def _data(**fields) -> bytes:
    """DATA 타입 스트림 메시지를 생성합니다. None 값은 제외합니다."""
    payload = {"type": "DATA"}
    payload.update({k: v for k, v in fields.items() if v is not None})
    return _stream_bytes(payload)


#-------------------------------------------------------------------------#
# DDL Helpers
#-------------------------------------------------------------------------#
def _list_ddl_files(dirs: dict) -> list:
    """DDL 디렉터리 내 파일 목록을 정렬하여 반환합니다."""
    try:
        ddl_dir = dirs['ddl']
        return [f for f in sorted(os.listdir(ddl_dir)) if os.path.isfile(os.path.join(ddl_dir, f))]
    except Exception:
        return []


#-------------------------------------------------------------------------#
# Understanding Flow Helpers
#-------------------------------------------------------------------------#
async def _ensure_folder_node(connection: Neo4jConnection, user_id: str, folder_name: str) -> None:
    """폴더 노드를 (user_id, name) 기준으로 없을 때만 생성합니다."""
    escaped_user_id = str(user_id).replace("'", r"\'")
    escaped_name = str(folder_name).replace("'", r"\'")
    query = f"MERGE (f:Folder {{user_id: '{escaped_user_id}', name: '{escaped_name}', has_children: true}}) RETURN f"
    await connection.execute_queries([query])

async def _already_analyzed_flow(connection: Neo4jConnection, user_id: str, file_pairs: list[tuple[str, str]]) -> AsyncGenerator[bytes, None]:
    """이미 분석된 경우 알림과 그래프 데이터를 스트림으로 반환합니다."""
    yield _alarm("ALREADY ANALYZED")
    graph_data = await connection.execute_query_and_return_graph(user_id, file_pairs)
    yield _data(graph=graph_data, analysis_progress=100)


async def _load_assets(dirs: dict, folder_name: str, file_name: str):
    """ANTLR 분석 JSON과 원본 PLSQL 내용을 비동기로 읽어 반환합니다.

    변경 사항:
    - file_name이 확장자를 포함해 전달되므로 추가 탐색 없이 그대로 사용합니다.
    - 분석 JSON은 전달된 파일의 베이스 이름(`analysis/{base}.json`)을 사용합니다.
    """
    folder_dir = os.path.join(dirs['plsql'], folder_name)
    plsql_file_path = os.path.join(folder_dir, file_name)
    base_name = os.path.splitext(file_name)[0]
    analysis_file_path = os.path.join(dirs['analysis'], f"{base_name}.json")

    async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as antlr_file, \
             aiofiles.open(plsql_file_path, 'r', encoding='utf-8') as plsql_file:
        antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())
        return json.loads(antlr_data), plsql_content


async def _run_understanding(
    dirs: dict,
    folder_name: str,
    file_name: str,
    file_pairs: list[tuple[str, str]],
    user_id: str,
    api_key: str,
    locale: str,
    connection: Neo4jConnection,
    events_from_analyzer: asyncio.Queue,
    events_to_analyzer: asyncio.Queue,
) -> AsyncGenerator[bytes, None]:
    """이해(understanding) 분석을 수행하고 단계별 스트림을 생성합니다."""
    antlr_data, plsql_content = await _load_assets(dirs, folder_name, file_name)
    last_line = len(plsql_content)
    plsql_numbered, _ = add_line_numbers(plsql_content)

    analyzer = Analyzer(
        antlr_data=antlr_data,
        file_content=plsql_numbered,
        send_queue=events_from_analyzer,
        receive_queue=events_to_analyzer,
        last_line=last_line,
        folder_name=folder_name,
        file_name=file_name,
        user_id=user_id,
        api_key=api_key,
        locale=locale,
    )
    analysis_task = asyncio.create_task(analyzer.run())

    while True:
        analysis_result = await events_from_analyzer.get()
        logging.info(f"Analysis Event: {folder_name}-{file_name}")

        if analysis_result.get('type') == 'end_analysis':
            logging.info(f"Understanding Completed for {folder_name}-{file_name}\n")
            # 파일 단위 후처리: 테이블 타입 변수 처리 (LLM 기반)
            # await resolve_table_variables_with_llm(connection, user_id, folder_name, file_name, api_key, locale)
            graph_result = await connection.execute_query_and_return_graph(user_id, file_pairs)
            yield _data(graph=graph_result, line_number=last_line, analysis_progress=100, current_file=f"{folder_name}-{file_name}")
            break

        if analysis_result.get('type') == 'error':
            logging.info(f"Understanding Failed for {file_name}")
            break

        cypher_queries = analysis_result.get('query_data', [])
        next_analysis_line = analysis_result['line_number']
        analysis_progress = int((next_analysis_line / last_line) * 100)
        await connection.execute_queries(cypher_queries)
        graph_result = await connection.execute_query_and_return_graph(user_id, file_pairs)
        yield _data(graph=graph_result, line_number=next_analysis_line, analysis_progress=analysis_progress, current_file=f"{folder_name}-{file_name}")
        await events_to_analyzer.put({'type': 'process_completed'})
        logging.info(f"Send Response for {file_name}")

    await analysis_task


async def postprocess_table_variables(connection: Neo4jConnection, user_id: str, folder_name: str, file_name: str, api_key: str, locale: str) -> None:
    """사후 처리:
    1) 변수 타입 해석(기존 로직 유지): 변수별로 테이블 메타를 가져와 타입을 결정하고 Variable 노드를 업데이트.
    2) 테이블 단위 컬럼 역할: 각 테이블당 컬럼/DML 요약을 모아 컬럼 역할을 도출하고 Column.description을 업데이트.
    """

    # 1) 변수 타입 해석 - 변수별 처리(기존 로직 단순화)
    fetch_vars = f"""
    MATCH (v:Variable {{folder_name: '{folder_name}', file_name: '{file_name}', user_id: '{user_id}'}})
    WHERE v.value STARTS WITH 'Table: '
    WITH v, trim(replace(v.value, 'Table: ', '')) AS fullName
    WITH v, CASE WHEN v.type CONTAINS '.' THEN split(v.type, '.') ELSE split(fullName, '.') END AS parts
    WITH v,
         CASE WHEN size(parts) = 2 THEN parts[0] ELSE null END AS schemaName,
         CASE WHEN size(parts) = 2 THEN parts[1] ELSE parts[0] END AS tableName
    MATCH (t:Table {{user_id: '{user_id}', name: toUpper(tableName)}})
    WHERE (schemaName IS NULL AND (t.schema IS NULL OR t.schema = '')) OR t.schema = toUpper(schemaName)
    OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{user_id}'}})
    WITH v, toUpper(schemaName) AS schema, toUpper(tableName) AS table,
         collect(DISTINCT {{name: c.name, dtype: coalesce(c.dtype, ''), nullable: toBoolean(c.nullable), comment: coalesce(c.description, '')}}) AS columns
    RETURN v.name AS varName, v.type AS declaredType, schema, table, columns
    """

    res_vars = await connection.execute_queries([fetch_vars])
    var_rows = res_vars[0] if res_vars else []
    if var_rows:
        update_queries = []
        type_tasks = []
        type_meta = []
        for row in var_rows:
            var_name = row.get('varName')
            declared_type = row.get('declaredType')
            schema = row.get('schema')
            table = row.get('table')
            columns_raw = row.get('columns')
            columns = json.loads(columns_raw) if isinstance(columns_raw, str) else (columns_raw or [])
            type_tasks.append(asyncio.create_task(
                resolve_table_variable_type(var_name, declared_type, schema, table, columns, api_key, locale)
            ))
            type_meta.append((var_name, declared_type))

        type_results = await asyncio.gather(*type_tasks)
        for (var_name, declared_type), result in zip(type_meta, type_results):
            resolved = (result or {}).get('resolvedType') or declared_type
            resolved_esc = resolved.replace("'", "\\'")
            var_name_esc = (var_name or '').replace("'", "\\'")
            update_queries.append(
                f"""
                MATCH (v:Variable {{name: '{var_name_esc}', folder_name: '{folder_name}', file_name: '{file_name}', user_id: '{user_id}'}})
                SET v.type = '{resolved_esc}', v.resolved = true
                """
            )

        if update_queries:
            await connection.execute_queries(update_queries)

    # 2) 테이블 단위 처리 - 테이블 하나씩 컬럼 역할 산출 후 Column.description 업데이트
    fetch_tables = f"""
    MATCH (folder:Folder {{user_id: '{user_id}', name: '{folder_name}'}})-[:CONTAINS]->(t:Table)
    OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{user_id}'}})
    OPTIONAL MATCH (dml)-[:FROM|:WRITES]->(t)
    WITH t, collect(DISTINCT dml.summary) AS dmlSummaries,
         collect(DISTINCT {{name: c.name, dtype: coalesce(c.dtype, ''), nullable: toBoolean(c.nullable), comment: coalesce(c.description, '')}}) AS columns
    RETURN coalesce(t.schema,'') AS schema, t.name AS table, columns, dmlSummaries
    """

    res_tables = await connection.execute_queries([fetch_tables])
    table_rows = res_tables[0] if res_tables else []
    if not table_rows:
        return

    column_update_queries = []
    roles_tasks = []
    roles_meta = []
    for row in table_rows:
        schema = row.get('schema') or ''
        table = row.get('table')
        columns_raw = row.get('columns')
        dml_summaries_raw = row.get('dmlSummaries')
        columns = json.loads(columns_raw) if isinstance(columns_raw, str) else (columns_raw or [])
        dml_summaries = json.loads(dml_summaries_raw) if isinstance(dml_summaries_raw, str) else (dml_summaries_raw or [])
        roles_tasks.append(asyncio.create_task(understand_column_roles(columns, dml_summaries, api_key, locale)))
        roles_meta.append((schema, table))

    roles_results = await asyncio.gather(*roles_tasks)
    for (schema, table), roles_result in zip(roles_meta, roles_results):
        schema_upper = (schema or '').upper()
        table_upper = (table or '').upper()
        match_table = (
            f"MATCH (t:Table {{user_id: '{user_id}', name: '{table_upper}', schema: '{schema_upper}'}})"
            if schema_upper else
            f"MATCH (t:Table {{user_id: '{user_id}', name: '{table_upper}'}})"
        )

        roles = (roles_result or {}).get('roles') or []
        for role_item in roles:
            col_name_esc = (role_item.get('name') or '').replace("'", "\\'")
            if not col_name_esc:
                continue
            desc_esc = (role_item.get('description') or '').replace("'", "\\'")
            column_update_queries.append(
                f"""
                {match_table}
                MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{user_id}', name: '{col_name_esc}'}})
                SET c.description = '{desc_esc}'
                """
            )

    if column_update_queries:
        await connection.execute_queries(column_update_queries)


#-------------------------------------------------------------------------#
# Understanding Entry Point
#-------------------------------------------------------------------------#
async def generate_and_execute_cypherQuery(file_names: list, user_id: str, api_key: str, locale: str) -> AsyncGenerator[Any, None]:
    """사이퍼 쿼리를 생성·실행하여 그래프 데이터 스트림을 반환합니다.

    매개변수:
    - file_names: (파일명, 객체명) 튜플 리스트
    - user_id: 사용자 ID
    - api_key: OpenAI 호환 API 키
    - locale: 로케일 문자열

    반환값:
    - AsyncGenerator: 그래프 데이터, 진행률 등을 포함한 스트림 청크
    """
    connection = Neo4jConnection()
    events_from_analyzer = asyncio.Queue()
    events_to_analyzer = asyncio.Queue()
    dirs = get_user_directories(user_id)

    try:
        file_pairs = [(fn, fl) for fn, fl in file_names]
        yield _alarm("Preparing Analysis Data")

        # ! 파일명 단 한개만 검사하고 있음 수정 필요 
        if await connection.node_exists(user_id, file_pairs):
            async for chunk in _already_analyzed_flow(connection, user_id, file_pairs):
                yield chunk
            return

        # DDL 결과를 LLM에 전달하지 않도록 제거합니다.
        ddl_files = _list_ddl_files(dirs)
        if ddl_files:
            for ddl_file_name in ddl_files:
                ddl_file_path = os.path.join(dirs['ddl'], ddl_file_name)
                yield _alarm("START DDL PROCESSING", file=ddl_file_name)
                logging.info(f"DDL 파일 처리 시작: {ddl_file_name}")
                try:
                    base_object_name = os.path.splitext(ddl_file_name)[0]
                    await process_ddl_and_table_nodes(ddl_file_path, connection, base_object_name, user_id, api_key, locale)
                except Exception as _e:
                    logging.error(f"DDL 파일 처리 실패: {ddl_file_name} - {_e}")
                    continue

        for folder_name, file_name in file_names:
            await _ensure_folder_node(connection, user_id, folder_name)
            async for chunk in _run_understanding(
                dirs,
                folder_name,
                file_name,
                file_pairs,
                user_id,
                api_key,
                locale,
                connection,
                events_from_analyzer,
                events_to_analyzer,
            ):
                yield chunk

        yield _alarm("ALL_ANALYSIS_COMPLETED")

    except UnderstandingError as e:
        yield _stream_bytes({"error": str(e)})
    except Exception as e:
        logging.exception(f"사이퍼쿼리 생성/실행 중 오류: {str(e)}")
        yield _stream_bytes({"error": str(e)})
    finally:
        await connection.close()



#-------------------------------------------------------------------------#
# DDL Processing
#-------------------------------------------------------------------------#
async def process_ddl_and_table_nodes(ddl_file_path: str, connection: Neo4jConnection, object_name: str, user_id: str, api_key: str, locale: str):
    """DDL을 분석하여 Table/Column 노드를 생성하고 관계를 구성합니다.

    - Table 노드 속성: description, name, schema, table_type, user_id
    - Column 노드 속성: description(comment), dtype, name, fqn, nullable, user_id
    - 관계:
      * (Table)-[:HAS_COLUMN]->(Column)
      * (Table)-[:FK_TO_TABLE]->(Table)
      * (Column)-[:FK_TO]->(Column)
    """

    try:
        async with aiofiles.open(ddl_file_path, 'r', encoding='utf-8') as ddl_file:
            ddl_content = await ddl_file.read()
            parsed = understand_ddl(ddl_content, api_key, locale)
            cypher_queries = []

            for table in parsed['analysis']:
                table_info = table['table']
                columns = table.get('columns', [])
                foreign_list = table.get('foreignKeys', [])
                primary_list = [str(pk).strip().upper() for pk in (table.get('primaryKeys') or []) if str(pk).strip()]

                schema_raw = (table_info.get('schema') or '').strip()
                schema_val = schema_raw.upper() if schema_raw else ''
                table_name_raw = (table_info.get('name') or '').strip()
                table_name_val = table_name_raw.upper()
                table_comment = (table_info.get('comment') or '').strip()
                table_type = (table_info.get('table_type') or 'BASE TABLE').strip().upper()

                # Table 노드 MERGE
                t_merge_key = {
                    'user_id': user_id,
                    'schema': schema_val,
                    'name': table_name_val,
                }
                t_merge_key_str = ', '.join(f"`{k}`: '{v}'" for k, v in t_merge_key.items())
                t_set_props = {
                    'description': table_comment.replace("'", "\\'"),
                    'table_type': table_type,
                }
                t_set_clause = ', '.join(f"t.`{k}` = '{v}'" for k, v in t_set_props.items())
                cypher_queries.append(f"MERGE (t:Table {{{t_merge_key_str}}}) SET {t_set_clause}")

                # Column 노드 MERGE 및 HAS_COLUMN
                for col in columns:
                    col_name_raw = (col.get('name') or '').strip()
                    if not col_name_raw:
                        continue
                    col_name = col_name_raw
                    dtype = (col.get('dtype') or col.get('type') or '').strip()
                    nullable_val = col.get('nullable', True)
                    description = (col.get('comment') or '').strip()
                    # fqn: schema.table.column (소문자)
                    fqn_parts = [schema_raw or '', table_name_raw, col_name]
                    fqn = '.'.join([p for p in fqn_parts if p]).lower()

                    c_merge_key = {
                        'user_id': user_id,
                        'name': col_name,
                        'fqn': fqn,
                    }
                    c_merge_key_str = ', '.join(f"`{k}`: '{v}'" for k, v in c_merge_key.items())
                    c_set_props = {
                        'dtype': dtype.replace("'", "\\'"),
                        'description': description.replace("'", "\\'"),
                        'nullable': str(bool(nullable_val)).lower(),
                    }
                    if col_name.upper() in primary_list:
                        c_set_props['pk_constraint'] = f"{table_name_raw}_pkey"
                    c_set_clause = ', '.join(f"c.`{k}` = '{v}'" for k, v in c_set_props.items())

                    cypher_queries.append(f"MERGE (c:Column {{{c_merge_key_str}}}) SET {c_set_clause}")
                    # HAS_COLUMN 관계
                    cypher_queries.append(
                        f"MATCH (t:Table {{{t_merge_key_str}}}), (c:Column {{{c_merge_key_str}}}) MERGE (t)-[:HAS_COLUMN]->(c)"
                    )

                # FK 관계 구성
                for fk in foreign_list:
                    src_col = (fk.get('column') or '').strip()
                    ref = (fk.get('ref') or '').strip()
                    if not src_col or not ref:
                        continue
                    # ref 형식: SCHEMA.TABLE.COLUMN 또는 TABLE.COLUMN
                    parts = [p.strip() for p in ref.split('.') if p.strip()]
                    if len(parts) == 3:
                        ref_schema, ref_table, ref_column = parts
                    elif len(parts) == 2:
                        ref_schema, ref_table, ref_column = schema_raw, parts[0], parts[1]
                    else:
                        continue

                    # 대상 Table
                    ref_table_merge_key = {
                        'user_id': user_id,
                        'schema': (ref_schema or '').upper(),
                        'name': (ref_table or '').upper(),
                    }
                    ref_table_merge_key_str = ', '.join(f"`{k}`: '{v}'" for k, v in ref_table_merge_key.items())
                    cypher_queries.append(f"MERGE (rt:Table {{{ref_table_merge_key_str}}})")
                    # FK_TO_TABLE
                    cypher_queries.append(
                        f"MATCH (t:Table {{{t_merge_key_str}}}), (rt:Table {{{ref_table_merge_key_str}}}) MERGE (t)-[:FK_TO_TABLE]->(rt)"
                    )

                    # Column FK_TO
                    src_fqn = '.'.join([p for p in [schema_raw or '', table_name_raw, src_col] if p]).lower()
                    ref_fqn = '.'.join([p for p in [ref_schema or '', ref_table, ref_column] if p]).lower()

                    src_c_key = { 'user_id': user_id, 'name': src_col, 'fqn': src_fqn }
                    ref_c_key = { 'user_id': user_id, 'name': ref_column, 'fqn': ref_fqn }
                    src_c_key_str = ', '.join(f"`{k}`: '{v}'" for k, v in src_c_key.items())
                    ref_c_key_str = ', '.join(f"`{k}`: '{v}'" for k, v in ref_c_key.items())

                    cypher_queries.append(f"MERGE (sc:Column {{{src_c_key_str}}})")
                    cypher_queries.append(f"MERGE (dc:Column {{{ref_c_key_str}}})")
                    cypher_queries.append(
                        f"MATCH (sc:Column {{{src_c_key_str}}}), (dc:Column {{{ref_c_key_str}}}) MERGE (sc)-[:FK_TO]->(dc)"
                    )

            await connection.execute_queries(cypher_queries)
            logging.info(f"DDL 파일 처리 완료: {object_name}")

    except UnderstandingError as e:
        raise
    except Exception as e:
        err_msg = f"DDL 파일 처리 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UnderstandingError(err_msg)
    


#-------------------------------------------------------------------------#
# Conversion: Spring Boot Generation
#-------------------------------------------------------------------------#
async def generate_spring_boot_project(file_names: list, user_id: str, api_key: str, locale: str) -> AsyncGenerator[Any, None]:
    """PL/SQL을 스프링 부트 프로젝트로 변환하고 산출물을 스트림으로 반환합니다.

    매개변수:
    - file_names: (파일명, 객체명) 튜플 리스트
    - user_id: 사용자 ID
    - api_key: OpenAI 호환 API 키
    - locale: 로케일 문자열

    반환값:
    - AsyncGenerator: 변환 단계 메시지와 산출물 코드 조각
    """
    try:
        def emit(data_type: str, **kwargs) -> bytes:
            return json.dumps({"data_type": data_type, **kwargs}).encode('utf-8') + b"send_stream"

        # 프로젝트 이름 생성
        project_name = await generate_project_name_prompt(file_names, api_key)
        logging.info(f"프로젝트 이름 생성 완료: {project_name}")
        yield emit("data", file_type="project_name", project_name=project_name)

        file_count = len(file_names)
        logging.info(f"변환할 파일 개수: {file_count}")

        for current_index, (file_name, object_name) in enumerate(file_names, start=1):
            # Step 1: Entity per object
            yield emit("message", step=1, content=f"{object_name} - Generating Entity Class")
            entity_result_list = await start_entity_processing([(file_name, object_name)], user_id, api_key, project_name, locale)
            for entity in entity_result_list:
                yield emit("data", file_type="entity_class", file_name=f"{entity['entityName']}.java", code=entity['entityCode'])
            yield emit("Done", step=1, file_count=file_count, current_count=current_index)

            # Step 2: Repository per object
            yield emit("message", step=2, content=f"{object_name} - Generating Repository Interface")
            used_query_methods, global_variables, sequence_methods, repository_list = await start_repository_processing(
                [(file_name, object_name)], user_id, api_key, project_name, locale
            )
            for repo in repository_list:
                yield emit("data", file_type="repository_class", file_name=f"{repo['repositoryName']}.java", code=repo['code'])
            yield emit("Done", step=2, file_count=file_count, current_count=current_index)

            yield emit("message", step=3, content="Business Logic Processing")
            logging.info(f"Start converting {object_name}\n")

            yield emit("message", step=3, content=f"{object_name} - Service Skeleton")
            service_creation_info, service_skeleton, service_class_name, exist_command_class, command_class_list = (
                await start_service_skeleton_processing(entity_result_list, object_name, global_variables, user_id, api_key, project_name, locale)
            )
            controller_skeleton, controller_class_name = await start_controller_skeleton_processing(object_name, exist_command_class, project_name)

            yield emit("message", step=3, content=f"{object_name} - Command Class")
            for command in command_class_list:
                yield emit("data", file_type="command_class", file_name=f"{command['commandName']}.java", code=command['commandCode'])
            yield emit("Done", step=3, file_count=file_count, current_count=current_index)

            yield emit("message", step=4, content=f"{object_name} - Service Controller Processing")
            merge_method_code = ""
            merge_controller_method_code = ""
            for svc in service_creation_info:
                variable_nodes, merged_java_code = await start_service_preprocessing(
                    svc['service_method_skeleton'],
                    svc['command_class_variable'],
                    svc['procedure_name'],
                    used_query_methods,
                    object_name,
                    sequence_methods,
                    user_id,
                    api_key,
                    locale,
                )

                if merged_java_code:
                    indented = textwrap.indent(merged_java_code.strip(), '        ')
                    completed = svc['method_skeleton_code'].replace("        CodePlaceHolder", "CodePlaceHolder").replace("CodePlaceHolder", indented)
                    merge_method_code = f"{merge_method_code}\n\n{completed}"
                else:
                    merge_method_code = await start_service_postprocessing(
                        svc['method_skeleton_code'], svc['procedure_name'], object_name, merge_method_code, user_id
                    )
                merge_controller_method_code = await start_controller_processing(
                    svc['method_signature'],
                    svc['procedure_name'],
                    svc['command_class_variable'],
                    svc['command_class_name'],
                    svc['node_type'],
                    merge_controller_method_code,
                    controller_skeleton,
                    object_name,
                    user_id,
                    api_key,
                    project_name,
                    locale,
                )

            service_code = await generate_service_class(service_skeleton, service_class_name, merge_method_code, user_id, project_name)
            controller_code = await generate_controller_class(controller_skeleton, controller_class_name, merge_controller_method_code, user_id, project_name)
            yield emit("data", file_type="service_class", file_name=f"{service_class_name}.java", code=service_code)
            yield emit("data", file_type="controller_class", file_name=f"{controller_class_name}.java", code=controller_code)

        yield emit("Done", step=4)

        yield emit("message", step=5, content="Generating pom.xml")
        pom_xml_code = await start_pomxml_processing(user_id, project_name)
        yield emit("data", file_type="pom", file_name="pom.xml", code=pom_xml_code)
        yield emit("Done", step=5)

        yield emit("message", step=6, content="Generating application.properties")
        properties_code = await start_APLproperties_processing(user_id, project_name)
        yield emit("data", file_type="properties", file_name="application.properties", code=properties_code)
        yield emit("Done", step=6)

        yield emit("message", step=7, content="Generating Main Application")
        main_code = await start_main_processing(user_id, project_name)
        yield emit("data", file_type="main", file_name=f"{project_name.capitalize()}Application.java", code=main_code)
        yield emit("Done", step=7)

    except ConvertingError as e:
        raise
    except Exception as e:
        err_msg = f"스프링 부트 프로젝트로 전환하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)


#-------------------------------------------------------------------------#
# File Ops
#-------------------------------------------------------------------------#
async def process_project_zipping(source_directory, output_zip_path):
    """생성된 스프링 부트 프로젝트 디렉터리를 ZIP 파일로 압축합니다.

    매개변수:
    - source_directory: 압축할 프로젝트 디렉터리 경로
    - output_zip_path: 생성될 ZIP 파일 경로
    """
    try:
        os.makedirs(os.path.dirname(output_zip_path), exist_ok=True)
        logging.info(f"Zipping contents of {source_directory} to {output_zip_path}")
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for current_path, _, files in os.walk(source_directory):
                for file in files:
                    file_path = os.path.join(current_path, file)
                    arcname = os.path.relpath(file_path, start=source_directory)
                    zipf.write(file_path, arcname)
        logging.info("Zipping completed successfully.")

    except Exception as e:
        err_msg = f"스프링부트 프로젝트를 Zip으로 압축하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise FileProcessingError(err_msg)
    

#-------------------------------------------------------------------------#
# Cleanup Ops
#-------------------------------------------------------------------------#
async def delete_all_temp_data(user_id:str):
    """임시 생성된 사용자 파일과 해당 Neo4j 데이터를 정리(삭제)합니다.

    매개변수:
    - user_id: 사용자 ID
    """
    neo4j = Neo4jConnection()
    
    try:
        user_base_dir = os.path.join(BASE_DIR, 'data', user_id)
        user_target_dir = os.path.join(BASE_DIR, 'target', 'java', user_id)
        dirs_to_delete = [user_base_dir, user_target_dir]
        for dir_path in dirs_to_delete:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
                os.makedirs(dir_path)
                logging.info(f"디렉토리 삭제 및 재생성 완료: {dir_path}")
        delete_query = [f"MATCH (n {{user_id: '{user_id}'}}) DETACH DELETE n"]
        await neo4j.execute_queries(delete_query)
        logging.info(f"Neo4J 데이터 초기화 완료 - User ID: {user_id}")
    
    except Neo4jError:
        raise
    except Exception as e:
        err_msg = f"파일 삭제 및 그래프 데이터 삭제 중 오류 발생: {str(e)}"
        logging.exception(err_msg)
        raise FileProcessingError(err_msg)


#-------------------------------------------------------------------------#
# External API Key Validation
#-------------------------------------------------------------------------#
async def validate_anthropic_api_key(api_key: str) -> bool:
    """OpenAI 호환 엔드포인트에 대해 간단 호출로 API 키 유효성을 검증합니다.

    매개변수:
    - api_key: 검증할 API 키

    반환값:
    - bool: 유효 시 True, 실패 시 False
    """
    try:
        llm = get_llm(max_tokens=8, api_key=api_key)
        result = (llm).invoke("ping")
        return bool(result)
    except Exception as e:
        logging.error(f"OpenAI 호환 키 검증 실패: {str(e)}")
        return False