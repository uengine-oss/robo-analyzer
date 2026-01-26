"""DDL 처리 Phase - 테이블/컬럼/스키마 노드 생성

dbms_analyzer.py에서 분리된 DDL 처리 로직입니다.
모든 로직은 100% 보존되며, 위치만 변경되었습니다.
"""

import asyncio
import logging
import os
from typing import Any, AsyncGenerator, Dict, List, Set, Tuple

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import AnalysisStats
from analyzer.ddl_static_parser import parse_ddl as regex_parse_ddl
from util.stream_event import (
    emit_data,
    emit_message,
    emit_phase_event,
)
from util.text_utils import (
    escape_for_cypher,
    log_process,
    parse_table_identifier,
    calculate_code_token,
)


def list_ddl_files(orchestrator: Any) -> List[str]:
    """DDL 파일 목록 조회
    
    DDL 디렉토리가 없거나 파일이 없으면 빈 리스트 반환 (경고 처리, 에러 아님)
    """
    ddl_dir = orchestrator.dirs.get("ddl", "")
    if not ddl_dir:
        log_process("ANALYZE", "DDL", "DDL 디렉토리 설정 없음 - DDL 처리 생략")
        return []
    if not os.path.isdir(ddl_dir):
        # DDL 디렉토리가 없으면 경고만 하고 빈 리스트 반환
        log_process("ANALYZE", "DDL", f"DDL 디렉토리 없음: {ddl_dir} - DDL 처리 생략")
        return []
    try:
        files = sorted(
            f for f in os.listdir(ddl_dir)
            if os.path.isfile(os.path.join(ddl_dir, f))
        )
        if not files:
            # DDL 파일이 없으면 경고만 하고 빈 리스트 반환
            log_process("ANALYZE", "DDL", f"DDL 디렉토리에 파일 없음: {ddl_dir} - DDL 처리 생략")
            return []
        log_process("ANALYZE", "DDL", f"DDL 파일 발견: {len(files)}개")
        return files
    except OSError as e:
        log_process("ANALYZE", "DDL", f"DDL 디렉토리 읽기 실패: {ddl_dir} - {e}")
        return []


def apply_name_case(name: str, name_case: str) -> str:
    """메타데이터 대소문자 변환 적용
    
    Args:
        name: 변환할 이름 (테이블명, 컬럼명, 스키마명 등)
        name_case: 변환 옵션 (original, uppercase, lowercase)
    
    Returns:
        변환된 이름
    """
    if not name:
        return name
    if name_case == "uppercase":
        return name.upper()
    elif name_case == "lowercase":
        return name.lower()
    return name  # original: 그대로 반환


def resolve_default_schema(
    directory: str,
    ddl_schemas: Set[str],
    name_case: str = 'original'
) -> str:
    """파일 경로에서 기본 스키마를 결정합니다.
    
    우선순위:
    1. 경로의 폴더명 중 DDL 스키마와 일치하는 것 (깊은 폴더 우선)
    2. 매칭 실패 시 파일이 존재하는 디렉토리명 사용
    
    Args:
        directory: 파일이 위치한 디렉토리 경로
        ddl_schemas: DDL에서 수집된 스키마 Set
        name_case: 대소문자 변환 옵션 (original, uppercase, lowercase)
    """
    if not directory:
        return apply_name_case("public", name_case)
    
    # 경로를 폴더 목록으로 분리 (깊은 순서대로)
    parts = directory.replace("\\", "/").split("/")
    parts = [p for p in parts if p]  # 빈 문자열 제거
    
    if not parts:
        return apply_name_case("public", name_case)
    
    # DDL 스키마가 있으면 매칭 시도 (깊은 폴더부터)
    # 대소문자 무관 비교 후, DDL에 저장된 원본 대소문자 반환
    if ddl_schemas:
        ddl_schemas_lower_map = {s.lower(): s for s in ddl_schemas}
        for folder in reversed(parts):
            matched = ddl_schemas_lower_map.get(folder.lower())
            if matched:
                return matched  # DDL에서 name_case 적용된 값 그대로 반환
    
    # 매칭 실패 시 파일이 존재하는 디렉토리명(가장 깊은 폴더)에 name_case 적용
    return apply_name_case(parts[-1], name_case)


async def process_ddl(
    ddl_path: str,
    client: Neo4jClient,
    file_name: str,
    orchestrator: Any,
    cypher_lock: asyncio.Lock,
    ddl_schemas: Set[str],
    ddl_table_metadata: Dict[Tuple[str, str], Dict[str, Any]],
    emit_progress: bool = True,
    file_base_progress: int = 0,
    file_end_progress: int = 100,
) -> AsyncGenerator[bytes | Tuple[dict, dict], None]:
    """DDL 파일 처리 및 테이블/컬럼 노드 생성 (스트리밍)
    
    정적 정규식 파서만 사용합니다 (LLM 파서 제거됨).
    
    Args:
        ddl_path: DDL 파일 경로
        client: Neo4j 클라이언트
        file_name: DDL 파일명
        orchestrator: 오케스트레이터 (설정 정보)
        cypher_lock: Cypher 쿼리 동시성 보호 락
        ddl_schemas: DDL에서 수집된 스키마 Set (mutable)
        ddl_table_metadata: DDL 테이블 메타데이터 캐시 (mutable)
        emit_progress: 진행 상황 메시지 emit 여부
        file_base_progress: 이 파일 처리 시작 시 전체 진행률 (0-100)
        file_end_progress: 이 파일 처리 완료 시 전체 진행률 (0-100)
    
    Yields:
        bytes: 진행 상황 메시지 (emit_message)
        tuple[dict, dict]: 최종 결과 (ddl_graph, ddl_stats) - 마지막에 한 번만
    """
    ddl_stats = {"tables": 0, "columns": 0, "fks": 0}
    
    # 진행률 범위 계산 (파일 내에서 파싱 50%, 저장 50% 비율)
    file_range = file_end_progress - file_base_progress
    parsing_end = file_base_progress + int(file_range * 0.5)
    saving_start = parsing_end  # 저장 시작 = 파싱 종료 시점
    saving_end = file_end_progress  # 저장 종료 = 파일 처리 완료 시점
    
    async with aiofiles.open(ddl_path, "r", encoding="utf-8") as f:
        ddl_content = await f.read()
    
    total_tokens = calculate_code_token(ddl_content)
    
    # ========================================
    # 정규식 파서 사용 (정적 파싱 - LLM 미사용)
    # ========================================
    if emit_progress:
        yield emit_message(f"   ⚡ 정규식 파서 사용 (정적 파싱)")
        yield emit_phase_event(
            phase_num=0,
            phase_name="DDL 처리",
            status="in_progress",
            progress=file_base_progress + int(file_range * 0.1),
            details={"mode": "regex", "tokens": total_tokens}
        )
    
    try:
        # 정규식 파서로 한 번에 파싱 (매우 빠름)
        parsed = await asyncio.to_thread(regex_parse_ddl, ddl_content)
        all_parsed_results = parsed.get("analysis", [])
        
        table_count = len(all_parsed_results)
        if emit_progress:
            # 처음 5개 테이블명 미리보기
            table_names = [t.get("table", {}).get("name", "?") for t in all_parsed_results[:5]]
            preview = ", ".join(table_names)
            if table_count > 5:
                preview += f" 외 {table_count - 5}개"
            
            yield emit_message(f"   ✅ 파싱 완료: {table_count}개 테이블 ({preview})")
            yield emit_phase_event(
                phase_num=0,
                phase_name="DDL 처리",
                status="in_progress",
                progress=parsing_end,
                details={"tables_parsed": table_count, "mode": "regex"}
            )
            
    except Exception as e:
        if emit_progress:
            yield emit_message(f"   ❌ 정규식 파싱 실패: {str(e)[:80]}")
        raise RuntimeError(f"DDL 정규식 파싱 실패: {e}")
    
    # 병합된 결과를 parsed로 사용
    parsed = {"analysis": all_parsed_results}
    
    # db 속성은 DML 처리(ast_processor)와 일관성을 위해 소문자로 변환
    db_name = (orchestrator.target or 'postgres').lower()
    
    # 대소문자 변환 옵션
    name_case = getattr(orchestrator, 'name_case', 'original')

    # ===========================================
    # UNWIND 배치용 데이터 수집 (개별 쿼리 대신)
    # ===========================================
    schemas_data = []  # 스키마 데이터
    tables_data = []   # 테이블 데이터
    columns_data = []  # 컬럼 데이터
    fks_data = []      # FK 관계 데이터
    
    # 중복 방지용 세트
    seen_schemas = set()
    seen_tables = set()

    for table_info in parsed.get("analysis", []):
        table = table_info.get("table", {})
        columns = table_info.get("columns", [])
        foreign_keys = table_info.get("foreignKeys", [])
        primary_keys = [
            str(pk).strip().upper()
            for pk in (table_info.get("primaryKeys") or [])
            if pk
        ]

        # 원본 값에서 따옴표 제거 후 대소문자 변환 적용
        schema_raw = (table.get("schema") or "").strip()
        table_name_raw = (table.get("name") or "").strip()
        comment = (table.get("comment") or "").strip()
        table_type = (table.get("table_type") or "BASE TABLE").strip().upper()
        
        # parse_table_identifier로 따옴표 제거 및 스키마/테이블 분리
        qualified = f"{schema_raw}.{table_name_raw}" if schema_raw else table_name_raw
        parsed_schema, parsed_name, _ = parse_table_identifier(qualified)
        
        # name_case 옵션에 따라 대소문자 변환 적용
        schema = apply_name_case(parsed_schema if parsed_schema else "public", name_case)
        parsed_name = apply_name_case(parsed_name, name_case)
        
        # DDL에서 발견된 스키마 수집 (name_case 적용된 값으로 저장)
        if schema and schema.lower() != 'public':
            ddl_schemas.add(schema)
        
        # 스키마 데이터 수집 (중복 방지)
        schema_key = (db_name, schema)
        if schema_key not in seen_schemas:
            seen_schemas.add(schema_key)
            schemas_data.append({
                "db": db_name,
                "name": schema
            })
        
        # 테이블 데이터 수집 (중복 방지)
        table_key = (db_name, schema, parsed_name)
        if table_key not in seen_tables:
            seen_tables.add(table_key)
            tables_data.append({
                "db": db_name,
                "schema": schema,
                "name": parsed_name,
                "description": escape_for_cypher(comment),
                "description_source": "ddl" if comment else "",
                "table_type": table_type
            })
            ddl_stats["tables"] += 1
        
        # DDL 메타데이터 캐시 저장 (메모리)
        column_metadata = {}
        for col in columns:
            col_name_raw = (col.get("name") or "").strip()
            if not col_name_raw:
                continue
            col_name = apply_name_case(col_name_raw, name_case)
            col_comment = (col.get("comment") or "").strip()
            column_metadata[col_name] = {
                "description": col_comment,
                "dtype": (col.get("dtype") or col.get("type") or "").strip(),
                "nullable": col.get("nullable", True),
            }
        
        cache_key = (schema.lower(), parsed_name.lower())
        ddl_table_metadata[cache_key] = {
            "description": comment,
            "columns": column_metadata,
            "original_schema": schema,
            "original_name": parsed_name,
        }

        # 컬럼 데이터 수집
        for col in columns:
            col_name_raw = (col.get("name") or "").strip()
            if not col_name_raw:
                continue
            
            col_name = apply_name_case(col_name_raw, name_case)
            col_type = (col.get("dtype") or col.get("type") or "").strip()
            col_nullable = col.get("nullable", True)
            col_comment = (col.get("comment") or "").strip()
            fqn = ".".join(filter(None, [schema, parsed_name, col_name])).lower()
            
            col_data = {
                "fqn": escape_for_cypher(fqn),
                "name": escape_for_cypher(col_name),
                "dtype": escape_for_cypher(col_type),
                "description": escape_for_cypher(col_comment),
                "description_source": "ddl" if col_comment else "",
                "nullable": col_nullable,
                "table_db": db_name,
                "table_schema": schema,
                "table_name": parsed_name
            }
            if col_name_raw.upper() in primary_keys:
                col_data["pk_constraint"] = f"{parsed_name}_pkey"
            
            columns_data.append(col_data)
            ddl_stats["columns"] += 1

        # FK 관계 데이터 수집
        for fk in foreign_keys:
            src_col_raw = (fk.get("column") or "").strip()
            ref = (fk.get("ref") or "").strip()
            if not src_col_raw or not ref or "." not in ref:
                continue

            ref_table_part, ref_col_raw = ref.rsplit(".", 1)
            ref_schema_parsed, ref_table_raw, _ = parse_table_identifier(ref_table_part)
            ref_schema_final = apply_name_case(ref_schema_parsed or schema, name_case)
            ref_table = apply_name_case(ref_table_raw, name_case)
            src_col = apply_name_case(src_col_raw, name_case)
            ref_col = apply_name_case(ref_col_raw, name_case)

            fks_data.append({
                "from_db": db_name,
                "from_schema": schema,
                "from_table": parsed_name,
                "from_column": escape_for_cypher(src_col),
                "to_db": db_name,
                "to_schema": ref_schema_final or "",
                "to_table": ref_table or "",
                "to_column": escape_for_cypher(ref_col)
            })
            ddl_stats["fks"] += 1

    # ===========================================
    # UNWIND 배치 실행 (7~8번의 Neo4j 호출로 완료!)
    # ===========================================
    if emit_progress:
        yield emit_message(f"   💾 UNWIND 배치 저장 시작: {ddl_stats['tables']}개 테이블, {ddl_stats['columns']}개 컬럼, {ddl_stats['fks']}개 FK")
        yield emit_phase_event(
            phase_num=0,
            phase_name="DDL 처리",
            status="in_progress",
            progress=saving_start,
            details={
                "step": "unwind_batch",
                "tables": ddl_stats['tables'],
                "columns": ddl_stats['columns'],
                "fks": ddl_stats['fks']
            }
        )
    
    all_nodes: dict = {}
    all_relationships: dict = {}
    
    # 1. 스키마 노드 생성
    if schemas_data:
        if emit_progress:
            yield emit_message(f"      📦 [1/6] 스키마 {len(schemas_data)}개 생성 중...")
        schema_query = """
        UNWIND $items AS item
        MERGE (__cy_s__:Schema {db: item.db, name: item.name})
        RETURN __cy_s__
        """
        async with cypher_lock:
            result = await client.run_batch_unwind(schema_query, schemas_data)
        for node in result.get("Nodes", []):
            all_nodes[node.get("Node ID")] = node
    
    # 2. 테이블 노드 생성
    if tables_data:
        if emit_progress:
            yield emit_message(f"      📦 [2/6] 테이블 {len(tables_data)}개 생성 중...")
        table_query = """
        UNWIND $items AS item
        MERGE (__cy_t__:Table {db: item.db, schema: item.schema, name: item.name})
        SET __cy_t__.description = item.description,
            __cy_t__.description_source = item.description_source,
            __cy_t__.table_type = item.table_type
        RETURN __cy_t__
        """
        async with cypher_lock:
            result = await client.run_batch_unwind(table_query, tables_data)
        for node in result.get("Nodes", []):
            all_nodes[node.get("Node ID")] = node
    
    # 3. 테이블-스키마 관계 생성
    if tables_data:
        if emit_progress:
            yield emit_message(f"      📦 [3/6] 테이블-스키마 관계 {len(tables_data)}개 생성 중...")
        belongs_query = """
        UNWIND $items AS item
        MATCH (__cy_t__:Table {db: item.db, schema: item.schema, name: item.name})
        MATCH (__cy_s__:Schema {db: item.db, name: item.schema})
        MERGE (__cy_t__)-[__cy_r__:BELONGS_TO]->(__cy_s__)
        RETURN __cy_t__, __cy_r__, __cy_s__
        """
        async with cypher_lock:
            result = await client.run_batch_unwind(belongs_query, tables_data)
        for node in result.get("Nodes", []):
            all_nodes[node.get("Node ID")] = node
        for rel in result.get("Relationships", []):
            all_relationships[rel.get("Relationship ID")] = rel
    
    # 4. 컬럼 노드 생성
    if columns_data:
        if emit_progress:
            yield emit_message(f"      📦 [4/6] 컬럼 {len(columns_data)}개 생성 중...")
        column_query = """
        UNWIND $items AS item
        MERGE (__cy_c__:Column {fqn: item.fqn})
        SET __cy_c__.name = item.name,
            __cy_c__.dtype = item.dtype,
            __cy_c__.description = item.description,
            __cy_c__.description_source = item.description_source,
            __cy_c__.nullable = item.nullable,
            __cy_c__.pk_constraint = CASE WHEN item.pk_constraint IS NOT NULL THEN item.pk_constraint ELSE __cy_c__.pk_constraint END
        RETURN __cy_c__
        """
        async with cypher_lock:
            result = await client.run_batch_unwind(column_query, columns_data)
        for node in result.get("Nodes", []):
            all_nodes[node.get("Node ID")] = node
    
    # 5. 테이블-컬럼 관계 생성
    if columns_data:
        if emit_progress:
            yield emit_message(f"      📦 [5/6] 테이블-컬럼 관계 {len(columns_data)}개 생성 중...")
        has_column_query = """
        UNWIND $items AS item
        MATCH (__cy_t__:Table {db: item.table_db, schema: item.table_schema, name: item.table_name})
        MATCH (__cy_c__:Column {fqn: item.fqn})
        MERGE (__cy_t__)-[__cy_r__:HAS_COLUMN]->(__cy_c__)
        RETURN __cy_t__, __cy_r__, __cy_c__
        """
        async with cypher_lock:
            result = await client.run_batch_unwind(has_column_query, columns_data)
        for node in result.get("Nodes", []):
            all_nodes[node.get("Node ID")] = node
        for rel in result.get("Relationships", []):
            all_relationships[rel.get("Relationship ID")] = rel
    
    # 6. FK 관계 생성 (참조 테이블 MERGE + FK 관계)
    if fks_data:
        if emit_progress:
            yield emit_message(f"      📦 [6/6] FK 관계 {len(fks_data)}개 생성 중...")
        # 먼저 참조 테이블이 없으면 생성
        ref_tables_query = """
        UNWIND $items AS item
        MERGE (__cy_rt__:Table {db: item.to_db, schema: item.to_schema, name: item.to_table})
        RETURN __cy_rt__
        """
        async with cypher_lock:
            result = await client.run_batch_unwind(ref_tables_query, fks_data)
        for node in result.get("Nodes", []):
            all_nodes[node.get("Node ID")] = node
        
        # FK 관계 생성
        fk_query = """
        UNWIND $items AS item
        MATCH (__cy_t__:Table {db: item.from_db, schema: item.from_schema, name: item.from_table})
        MATCH (__cy_rt__:Table {db: item.to_db, schema: item.to_schema, name: item.to_table})
        MERGE (__cy_t__)-[__cy_r__:FK_TO_TABLE {sourceColumn: item.from_column, targetColumn: item.to_column}]->(__cy_rt__)
        ON CREATE SET __cy_r__.type = 'many_to_one', __cy_r__.source = 'ddl'
        RETURN __cy_t__, __cy_r__, __cy_rt__
        """
        async with cypher_lock:
            result = await client.run_batch_unwind(fk_query, fks_data)
        for node in result.get("Nodes", []):
            all_nodes[node.get("Node ID")] = node
        for rel in result.get("Relationships", []):
            all_relationships[rel.get("Relationship ID")] = rel
    
    if emit_progress:
        yield emit_message(f"   ✅ UNWIND 배치 저장 완료: {len(all_nodes)}개 노드, {len(all_relationships)}개 관계")
        yield emit_phase_event(
            phase_num=0,
            phase_name="DDL 처리",
            status="in_progress",
            progress=saving_end,
            details={
                "step": "unwind_completed",
                "nodes_created": len(all_nodes),
                "relationships_created": len(all_relationships)
            }
        )
    
    result = {
        "Nodes": list(all_nodes.values()),
        "Relationships": list(all_relationships.values())
    }
    
    if emit_progress:
        yield emit_message(f"   ✅ Neo4j 저장 완료: {len(result['Nodes'])}개 노드, {len(result['Relationships'])}개 관계 생성")
        yield emit_phase_event(
            phase_num=0,
            phase_name="DDL 처리",
            status="in_progress",
            progress=saving_end,
            details={
                "step": "neo4j_saved",
                "tables": ddl_stats['tables'],
                "columns": ddl_stats['columns'],
                "fks": ddl_stats['fks'],
                "nodes_created": len(result['Nodes']),
                "relationships_created": len(result['Relationships'])
            }
        )
    
    log_process("ANALYZE", "DDL", f"DDL 처리 완료: {file_name} (T:{ddl_stats['tables']}, C:{ddl_stats['columns']}, FK:{ddl_stats['fks']})")
    
    # 최종 결과를 특별한 형태로 yield (tuple)
    yield (result, ddl_stats)


async def run_ddl_phase(
    analyzer: Any,
    client: Neo4jClient,
    orchestrator: Any,
    stats: AnalysisStats,
) -> AsyncGenerator[bytes, None]:
    """DDL 파일 처리 - 테이블/컬럼 스키마 생성
    
    Args:
        analyzer: DbmsAnalyzer 인스턴스 (emit_* 메서드, 공유 상태 접근용)
        client: Neo4j 클라이언트
        orchestrator: 오케스트레이터
        stats: 분석 통계
    """
    ddl_files = list_ddl_files(orchestrator)
    
    if not ddl_files:
        yield analyzer.emit_skip("DDL 파일 없음 → 스키마 처리 건너뜀")
        return
    
    ddl_count = len(ddl_files)
    yield emit_message("")
    yield analyzer.emit_separator()
    yield analyzer.emit_phase_header(0, "📋 DDL 스키마 수집", f"{ddl_count}개 DDL")
    yield analyzer.emit_separator()
    
    ddl_dir = orchestrator.dirs["ddl"]
    
    for idx, ddl_file in enumerate(ddl_files, 1):
        yield emit_message("")
        yield analyzer.emit_file_start(idx, ddl_count, ddl_file)
        
        # 파일 단위 진행률: 각 파일이 (idx-1)/ddl_count ~ idx/ddl_count 구간 차지
        file_base_progress = int(((idx - 1) / ddl_count) * 100)
        file_end_progress = int((idx / ddl_count) * 100)
        
        # _process_ddl은 이제 AsyncGenerator - 메시지와 최종 결과를 yield
        ddl_graph = None
        ddl_stats_file = {"tables": 0, "columns": 0, "fks": 0}
        
        async for item in process_ddl(
            ddl_path=os.path.join(ddl_dir, ddl_file),
            client=client,
            file_name=ddl_file,
            orchestrator=orchestrator,
            cypher_lock=analyzer._cypher_lock,
            ddl_schemas=analyzer._ddl_schemas,
            ddl_table_metadata=analyzer._ddl_table_metadata,
            emit_progress=True,
            file_base_progress=file_base_progress,
            file_end_progress=file_end_progress,
        ):
            if isinstance(item, tuple):
                # 최종 결과 (ddl_graph, ddl_stats)
                ddl_graph, ddl_stats_file = item
            else:
                # 진행 상황 메시지 (bytes)
                yield item
        
        if ddl_stats_file["tables"]:
            yield emit_message(f"   ✓ Table 노드: {ddl_stats_file['tables']}개")
        if ddl_stats_file["columns"]:
            yield emit_message(f"   ✓ Column 노드: {ddl_stats_file['columns']}개")
        if ddl_stats_file["fks"]:
            yield emit_message(f"   ✓ FK 관계: {ddl_stats_file['fks']}개")
        
        # 파일 완료 시 진행률 업데이트
        yield emit_phase_event(0, "DDL 처리", "running", file_end_progress)
        
        stats.add_ddl_result(ddl_stats_file["tables"], ddl_stats_file["columns"], ddl_stats_file["fks"])
        
        if ddl_graph and (ddl_graph.get("Nodes") or ddl_graph.get("Relationships")):
            yield emit_data(
                graph=ddl_graph,
                line_number=0,
                analysis_progress=0,
                current_file=f"DDL-{ddl_file}",
            )
    
    yield emit_message("")
    yield emit_message("📊 DDL 처리 완료:")
    yield emit_message(f"   • 테이블: {stats.ddl_tables}개")
    yield emit_message(f"   • 컬럼: {stats.ddl_columns}개")
    yield emit_message(f"   • FK: {stats.ddl_fks}개")

