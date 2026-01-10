"""ROBO Analyzer API 라우터

엔드포인트:
- POST /robo/analyze/      : 소스 파일 분석 → Neo4j 그래프 생성
- POST /robo/detect-types/ : 파일 내용 기반 타입 자동 감지
- GET /robo/check-data/    : Neo4j에 기존 데이터 존재 여부 확인
- DELETE /robo/data/       : 사용자 데이터 전체 삭제
- GET /robo/lineage/       : 데이터 리니지 그래프 조회
- POST /robo/lineage/analyze/ : ETL 코드에서 리니지 추출
- GET /robo/schema/tables  : 테이블 목록 조회 (Neo4j)
- GET /robo/schema/tables/{table_name}/columns : 테이블 컬럼 목록 조회
- GET /robo/schema/relationships : 테이블 관계 목록 조회
- POST /robo/schema/relationships : 테이블 관계 추가
- DELETE /robo/schema/relationships : 테이블 관계 삭제
"""

import logging
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.orchestrator import (
    AnalysisOrchestrator,
    create_orchestrator,
    extract_user_id,
)
from analyzer.neo4j_client import Neo4jClient
from analyzer.lineage_analyzer import LineageAnalyzer, analyze_lineage_from_sql
from config.settings import settings
from util.stream_utils import build_error_body, stream_with_error_boundary
from util.file_type_detector import detect_batch_file_types
from util.utility_tool import escape_for_cypher


router = APIRouter(prefix=settings.api_prefix)
logger = logging.getLogger(__name__)


# =============================================================================
# 요청/응답 모델
# =============================================================================

class FileContent(BaseModel):
    """파일 내용"""
    fileName: str
    content: str


class DetectTypesRequest(BaseModel):
    """파일 타입 감지 요청"""
    files: list[FileContent]


# =============================================================================
# 파일 타입 감지 API
# =============================================================================

@router.post("/detect-types/")
async def detect_file_types(request: DetectTypesRequest):
    """파일 내용을 분석하여 소스 코드 타입 자동 감지
    
    Request Body:
        files: [{ fileName: string, content: string }, ...]
    
    Response: JSON
        files: [{ fileName, fileType, confidence, details, suggestedStrategy, suggestedTarget }, ...]
        summary: {
            total: int,
            byType: { java: 5, oracle_sp: 3, ... },
            suggestedStrategy: "framework" | "dbms",
            suggestedTarget: "java" | "oracle" | "postgresql" | "python"
        }
    """
    logger.info("[API] 파일 타입 감지 요청 | files=%d", len(request.files))
    
    try:
        files_data = [(f.fileName, f.content) for f in request.files]
        result = detect_batch_file_types(files_data)
        
        logger.info(
            "[API] 파일 타입 감지 완료 | total=%d | strategy=%s | target=%s",
            result["summary"]["total"],
            result["summary"]["suggestedStrategy"],
            result["summary"]["suggestedTarget"],
        )
        
        return result
    except Exception as e:
        logger.error("[API] 파일 타입 감지 실패 | error=%s", e)
        raise HTTPException(500, f"파일 타입 감지 실패: {e}")


# =============================================================================
# 분석 API
# =============================================================================

@router.post("/analyze/")
async def analyze_source_code(request: Request):
    """소스 파일을 분석하여 Neo4j 그래프 데이터 생성
    
    Request Headers:
        Session-UUID: 사용자 세션 ID (필수)
        OpenAI-Api-Key: LLM API 키 (필수)
        Accept-Language: 출력 언어 (기본: ko)
    
    Request Body:
        projectName: 프로젝트명 (필수)
        strategy: "framework" | "dbms" (기본: framework)
        target: "java" | "oracle" | ... (기본: java)
    
    Response: NDJSON 스트림
    """
    body = await request.json()
    orchestrator = await create_orchestrator(request, body)
    
    file_names = orchestrator.discover_source_files()
    has_ddl = orchestrator.has_ddl_files()
    
    # 소스 파일도 없고 DDL도 없으면 오류
    if not file_names and not has_ddl:
        raise HTTPException(400, "분석할 소스 파일 또는 DDL이 없습니다.")

    logger.info(
        "[API] 분석 시작 | project=%s | strategy=%s | files=%d | has_ddl=%s",
        orchestrator.project_name,
        orchestrator.strategy,
        len(file_names),
        has_ddl,
    )

    return StreamingResponse(
        stream_with_error_boundary(orchestrator.run_analysis(file_names)),
        media_type="application/x-ndjson",
    )


@router.get("/check-data/")
async def check_existing_data(request: Request):
    """Neo4j에 기존 데이터 존재 여부 확인
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
        hasData: bool - 기존 데이터 존재 여부
        nodeCount: int - 노드 개수
    """
    user_id = extract_user_id(request)
    logger.info("[API] 데이터 존재 확인 요청 | user=%s", user_id)

    client = Neo4jClient()
    try:
        result = await client.execute_queries([
            f"MATCH (n {{user_id: '{user_id}'}}) RETURN count(n) as count"
        ])
        node_count = result[0][0]["count"] if result and result[0] else 0
        
        return {
            "hasData": node_count > 0,
            "nodeCount": node_count
        }
    except Exception as e:
        logger.error("[API] 데이터 확인 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/graph/")
async def get_graph_data(request: Request):
    """Neo4j에서 기존 그래프 데이터 조회
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
        Nodes: [{ "Node ID", "Labels", "Properties" }, ...]
        Relationships: [{ "Relationship ID", "Start Node ID", "End Node ID", "Type", "Properties" }, ...]
    """
    user_id = extract_user_id(request)
    logger.info("[API] 그래프 데이터 조회 요청 | user=%s", user_id)

    client = Neo4jClient()
    try:
        # 노드 조회 (전체 노드 - user_id 필터 제거)
        node_query = """
            MATCH (n)
            RETURN elementId(n) AS nodeId, labels(n) AS labels, properties(n) AS props
        """
        
        # 관계 조회 (전체 관계 - user_id 필터 제거)
        rel_query = """
            MATCH (a)-[r]->(b)
            RETURN elementId(r) AS relId, 
                   elementId(a) AS startId, 
                   elementId(b) AS endId, 
                   type(r) AS relType, 
                   properties(r) AS props
        """
        
        results = await client.execute_queries([node_query, rel_query])
        node_result = results[0] if results else []
        rel_result = results[1] if len(results) > 1 else []
        
        # 응답 형식 변환 (분석 API와 동일한 형식)
        nodes = []
        for record in node_result:
            nodes.append({
                "Node ID": record["nodeId"],
                "Labels": record["labels"],
                "Properties": record["props"]
            })
        
        relationships = []
        for record in rel_result:
            relationships.append({
                "Relationship ID": record["relId"],
                "Start Node ID": record["startId"],
                "End Node ID": record["endId"],
                "Type": record["relType"],
                "Properties": record["props"]
            })
        
        logger.info(
            "[API] 그래프 데이터 조회 완료 | nodes=%d | relationships=%d",
            len(nodes), len(relationships)
        )
        
        return {
            "Nodes": nodes,
            "Relationships": relationships
        }
    except Exception as e:
        logger.error("[API] 그래프 조회 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/graph/related-tables/{table_name}")
async def get_related_tables(table_name: str, request: Request):
    """특정 테이블과 연결된 모든 테이블 조회 (FK_TO_TABLE 관계 포함)
    
    Path Params:
        table_name: 기준 테이블명
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
        tables: [{ name, schema, description }, ...]
        relationships: [{ from_table, to_table, type, column_pairs: [{source, target}] }, ...]
    """
    user_id = extract_user_id(request)
    logger.info("[API] 관련 테이블 조회 요청 | user=%s | table=%s", user_id, table_name)
    
    client = Neo4jClient()
    try:
        safe_table_name = table_name.replace("'", "\\'").replace('"', '\\"')
        
        # 1. FK_TO_TABLE 관계 조회 (sourceColumn, targetColumn, source 포함)
        # 새 구조: 각 FK_TO_TABLE 관계마다 sourceColumn, targetColumn 속성 (단일 값)
        fk_query = f"""
            MATCH (t1:Table)-[r:FK_TO_TABLE]->(t2:Table)
            WHERE t1.name = '{safe_table_name}' OR t2.name = '{safe_table_name}'
               OR t1.fqn ENDS WITH '{safe_table_name}' OR t2.fqn ENDS WITH '{safe_table_name}'
            RETURN t1.name AS from_table, 
                   t1.schema_name AS from_schema,
                   t1.description AS from_desc,
                   t2.name AS to_table, 
                   t2.schema_name AS to_schema,
                   t2.description AS to_desc,
                   r.sourceColumn AS source_column,
                   r.targetColumn AS target_column,
                   COALESCE(r.source, 'ddl') AS source,
                   type(r) AS rel_type
        """
        
        # 2. 같은 프로시저에서 참조되는 테이블 (CO_REFERENCED)
        proc_query = f"""
            MATCH (t:Table)
            WHERE t.name = '{safe_table_name}' OR t.fqn ENDS WITH '{safe_table_name}'
            
            OPTIONAL MATCH (t)<-[:FROM|WRITES]-(s1)<-[:PARENT_OF*]-(proc)
            OPTIONAL MATCH (proc)-[:PARENT_OF*]->(s2)-[:FROM|WRITES]->(t2:Table)
            WHERE t2 <> t
            
            WITH t, COLLECT(DISTINCT {{
                name: t2.name, 
                schema: t2.schema_name, 
                description: t2.description
            }}) AS proc_related
            
            RETURN t.name AS base_table, 
                   t.schema_name AS base_schema,
                   proc_related
        """
        
        fk_results = await client.execute_queries([fk_query])
        proc_results = await client.execute_queries([proc_query])
        
        fk_result = fk_results[0] if fk_results else []
        proc_result = proc_results[0] if proc_results else []
        
        tables = []
        relationships = []
        seen_tables = set()
        seen_rels = set()
        
        # 기준 테이블 추가
        seen_tables.add(table_name)
        
        # FK_TO_TABLE 관계 처리 (sourceColumn/targetColumn/source 정보 포함)
        # 새 구조: 각 FK 관계마다 하나의 sourceColumn, targetColumn
        # 같은 테이블 간에 여러 FK 관계가 있을 수 있으므로 column_pairs로 그룹화
        fk_by_table_pair = {}  # (from_table, to_table) -> { source, column_pairs }
        
        for record in fk_result:
            from_table = record.get("from_table")
            to_table = record.get("to_table")
            source_column = record.get("source_column") or ""
            target_column = record.get("target_column") or ""
            source_type = record.get("source") or "ddl"  # ddl, procedure, user
            
            # 테이블 추가
            if from_table and from_table not in seen_tables:
                seen_tables.add(from_table)
                tables.append({
                    "name": from_table,
                    "schema": record.get("from_schema") or "public",
                    "description": record.get("from_desc")
                })
            
            if to_table and to_table not in seen_tables:
                seen_tables.add(to_table)
                tables.append({
                    "name": to_table,
                    "schema": record.get("to_schema") or "public",
                    "description": record.get("to_desc")
                })
            
            # 테이블 페어별로 column_pairs 그룹화
            pair_key = (from_table, to_table)
            if pair_key not in fk_by_table_pair:
                fk_by_table_pair[pair_key] = {
                    "source": source_type,
                    "column_pairs": []
                }
            
            # 컬럼 페어 추가 (비어있지 않은 경우만)
            if source_column or target_column:
                fk_by_table_pair[pair_key]["column_pairs"].append({
                    "source": source_column,
                    "target": target_column
                })
        
        # 관계 리스트로 변환
        for (from_table, to_table), data in fk_by_table_pair.items():
            rel_key = f"{from_table}->{to_table}"
            if rel_key not in seen_rels:
                seen_rels.add(rel_key)
                relationships.append({
                    "from_table": from_table,
                    "to_table": to_table,
                    "type": "FK_TO_TABLE",
                    "source": data["source"],  # ddl, procedure, user
                    "column_pairs": data["column_pairs"]
                })
        
        # CO_REFERENCED 관계 처리 (프로시저 분석에서 발견된 관계)
        for record in proc_result:
            base_table = record.get("base_table")
            for item in record.get("proc_related", []):
                if item.get("name") and item["name"] not in seen_tables:
                    seen_tables.add(item["name"])
                    tables.append({
                        "name": item["name"],
                        "schema": item.get("schema") or "public",
                        "description": item.get("description")
                    })
                    
                    rel_key = f"{base_table}->{item['name']}"
                    if rel_key not in seen_rels:
                        seen_rels.add(rel_key)
                        relationships.append({
                            "from_table": base_table,
                            "to_table": item["name"],
                            "type": "CO_REFERENCED",
                            "source": "procedure",  # 프로시저 분석에서 발견
                            "column_pairs": []
                        })
        
        logger.info("[API] 관련 테이블 조회 완료 | table=%s | tables=%d | rels=%d", 
                    table_name, len(tables), len(relationships))
        
        return {
            "base_table": table_name,
            "tables": tables,
            "relationships": relationships
        }
    except Exception as e:
        logger.error("[API] 관련 테이블 조회 실패 | table=%s | error=%s", table_name, e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.delete("/delete/")
async def delete_user_data(request: Request, include_files: bool = False):
    """사용자 데이터 삭제
    
    기본적으로 Neo4j 그래프 데이터만 삭제하고 파일은 유지합니다.
    파일도 함께 삭제하려면 include_files=true 파라미터를 전달하세요.
    
    Query Params:
        include_files: 파일 시스템도 삭제할지 여부 (기본값: false)
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
    """
    user_id = extract_user_id(request)
    logger.info("[API] 데이터 삭제 요청 | user=%s | include_files=%s", user_id, include_files)

    try:
        orchestrator = AnalysisOrchestrator(
            user_id=user_id,
            api_key="",
            locale="",
            project_name="",
        )
        
        if include_files:
            await orchestrator.cleanup_all_data(include_files=True)
            logger.info("[API] 전체 데이터 삭제 완료 (파일 포함) | user=%s", user_id)
            return {"message": "모든 데이터(파일 + Neo4j)가 삭제되었습니다."}
        else:
            await orchestrator.cleanup_neo4j_data()
            logger.info("[API] Neo4j 데이터만 삭제 완료 | user=%s", user_id)
            return {"message": "Neo4j 그래프 데이터가 삭제되었습니다. (파일은 유지됨)"}
    except Exception as e:
        logger.error("[API] 데이터 삭제 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))


# =============================================================================
# 데이터 리니지 API
# =============================================================================

class LineageAnalyzeRequest(BaseModel):
    """데이터 리니지 분석 요청"""
    projectName: str
    sqlContent: str
    fileName: str = ""
    dbms: str = "oracle"


class LineageNode(BaseModel):
    """리니지 노드"""
    id: str
    name: str
    type: str  # SOURCE, TARGET, ETL
    properties: dict = {}


class LineageEdge(BaseModel):
    """리니지 엣지"""
    id: str
    source: str
    target: str
    type: str  # DATA_FLOW_TO, TRANSFORMS_TO
    properties: dict = {}


class LineageGraphResponse(BaseModel):
    """데이터 리니지 그래프 응답"""
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    stats: dict = {}


@router.get("/lineage/")
async def get_lineage_graph(request: Request, projectName: Optional[str] = None):
    """데이터 리니지 그래프 조회
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Query Params:
        projectName: 프로젝트명 (선택, 지정시 해당 프로젝트만)
    
    Response: JSON
        nodes: [{ id, name, type, properties }, ...]
        edges: [{ id, source, target, type, properties }, ...]
        stats: { etlCount, sourceCount, targetCount, flowCount }
    """
    user_id = extract_user_id(request)
    logger.info("[API] 리니지 조회 요청 | user=%s | project=%s", user_id, projectName)
    
    client = Neo4jClient()
    try:
        # 노드 조회 쿼리
        where_clause = f"n.user_id = '{escape_for_cypher(user_id)}'"
        if projectName:
            where_clause += f" AND n.project_name = '{escape_for_cypher(projectName)}'"
        
        # DataSource, ETLProcess 노드 조회
        node_query = f"""
            MATCH (n)
            WHERE ({where_clause})
              AND (n:DataSource OR n:ETLProcess)
            RETURN n.name AS name,
                   labels(n)[0] AS nodeType,
                   elementId(n) AS id,
                   properties(n) AS properties
            ORDER BY nodeType, name
        """
        
        # 관계 조회 쿼리
        rel_query = f"""
            MATCH (src)-[r]->(tgt)
            WHERE src.user_id = '{escape_for_cypher(user_id)}'
              AND tgt.user_id = '{escape_for_cypher(user_id)}'
              AND (src:DataSource OR src:ETLProcess)
              AND (tgt:DataSource OR tgt:ETLProcess)
              AND type(r) IN ['DATA_FLOW_TO', 'TRANSFORMS_TO']
        """
        if projectName:
            rel_query += f" AND src.project_name = '{escape_for_cypher(projectName)}'"
        
        rel_query += """
            RETURN elementId(r) AS id,
                   elementId(src) AS source,
                   elementId(tgt) AS target,
                   type(r) AS relType,
                   properties(r) AS properties
        """
        
        # 통계 쿼리
        stats_query = f"""
            MATCH (n)
            WHERE n.user_id = '{escape_for_cypher(user_id)}'
        """
        if projectName:
            stats_query += f" AND n.project_name = '{escape_for_cypher(projectName)}'"
        
        stats_query += """
            WITH 
                sum(CASE WHEN n:ETLProcess THEN 1 ELSE 0 END) AS etlCount,
                sum(CASE WHEN n:DataSource AND n.type = 'SOURCE' THEN 1 ELSE 0 END) AS sourceCount,
                sum(CASE WHEN n:DataSource AND n.type = 'TARGET' THEN 1 ELSE 0 END) AS targetCount
            RETURN etlCount, sourceCount, targetCount
        """
        
        # 쿼리 실행
        node_result, rel_result, stats_result = await client.execute_queries([
            node_query, rel_query, stats_query
        ])
        
        # 응답 변환
        nodes = []
        for record in node_result:
            node_type = record.get("nodeType", "Unknown")
            if node_type == "DataSource":
                node_type = record.get("properties", {}).get("type", "SOURCE")
            elif node_type == "ETLProcess":
                node_type = "ETL"
            
            nodes.append({
                "id": record["id"],
                "name": record["name"],
                "type": node_type,
                "properties": record.get("properties", {})
            })
        
        edges = []
        for record in rel_result:
            edges.append({
                "id": record["id"],
                "source": record["source"],
                "target": record["target"],
                "type": record["relType"],
                "properties": record.get("properties", {})
            })
        
        stats = {}
        if stats_result:
            stats = {
                "etlCount": stats_result[0].get("etlCount", 0),
                "sourceCount": stats_result[0].get("sourceCount", 0),
                "targetCount": stats_result[0].get("targetCount", 0),
                "flowCount": len(edges)
            }
        
        logger.info(
            "[API] 리니지 조회 완료 | nodes=%d | edges=%d",
            len(nodes), len(edges)
        )
        
        return {
            "nodes": nodes,
            "edges": edges,
            "stats": stats
        }
    
    except Exception as e:
        logger.error("[API] 리니지 조회 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.post("/lineage/analyze/")
async def analyze_lineage(request: Request, body: LineageAnalyzeRequest):
    """ETL 코드에서 데이터 리니지 추출 및 Neo4j 저장
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Request Body:
        projectName: 프로젝트명 (필수)
        sqlContent: SQL 소스 코드 (필수)
        fileName: 파일명 (선택)
        dbms: DBMS 타입 (기본: oracle)
    
    Response: JSON
        lineages: [{ etl_name, source_tables, target_tables, operation_type }, ...]
        stats: { etl_nodes, data_sources, data_flows }
    """
    user_id = extract_user_id(request)
    logger.info(
        "[API] 리니지 분석 요청 | user=%s | project=%s | file=%s",
        user_id, body.projectName, body.fileName
    )
    
    try:
        lineage_list, stats = await analyze_lineage_from_sql(
            sql_content=body.sqlContent,
            user_id=user_id,
            project_name=body.projectName,
            file_name=body.fileName,
            dbms=body.dbms,
        )
        
        # 응답 변환
        lineages = [
            {
                "etl_name": l.etl_name,
                "source_tables": l.source_tables,
                "target_tables": l.target_tables,
                "operation_type": l.operation_type,
            }
            for l in lineage_list
        ]
        
        logger.info(
            "[API] 리니지 분석 완료 | lineages=%d | etl=%d | sources=%d | flows=%d",
            len(lineages),
            stats.get("etl_nodes", 0),
            stats.get("data_sources", 0),
            stats.get("data_flows", 0),
        )
        
        return {
            "lineages": lineages,
            "stats": stats
        }
    
    except Exception as e:
        logger.error("[API] 리니지 분석 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))


# =============================================================================
# 스키마 API (ERD 모델링용)
# =============================================================================

class SchemaTableInfo(BaseModel):
    """테이블 정보"""
    name: str
    table_schema: str  # Renamed from 'schema' to avoid BaseModel attribute conflict
    description: str
    column_count: int
    project_name: Optional[str] = None


class SchemaColumnInfo(BaseModel):
    """컬럼 정보"""
    name: str
    table_name: str
    dtype: str
    nullable: bool
    description: str


class SchemaRelationshipInfo(BaseModel):
    """테이블 관계 정보"""
    from_table: str
    from_schema: str
    from_column: str
    to_table: str
    to_schema: str
    to_column: str
    relationship_type: str
    description: str


class AddRelationshipRequest(BaseModel):
    """관계 추가 요청"""
    from_table: str
    from_schema: str = ""
    from_column: str
    to_table: str
    to_schema: str = ""
    to_column: str
    relationship_type: str = "FK_TO_TABLE"
    description: str = ""


class SemanticSearchRequest(BaseModel):
    """시멘틱 검색 요청"""
    query: str
    project_name: Optional[str] = None
    limit: int = 10


class SemanticSearchResult(BaseModel):
    """시멘틱 검색 결과"""
    name: str
    schema: str
    description: str
    similarity: float  # 유사도 점수 (0~1)


@router.post("/schema/semantic-search")
async def semantic_search_tables(
    request: Request,
    body: SemanticSearchRequest
):
    """시멘틱 검색: 테이블 설명의 의미적 유사도 기반 검색
    
    OpenAI 임베딩을 사용하여 검색 쿼리와 테이블 설명 간의 유사도를 계산합니다.
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
        X-API-Key: OpenAI API 키 (선택, 없으면 환경변수 사용)
    
    Request Body:
        query: 검색 쿼리
        project_name: 프로젝트명 필터 (선택)
        limit: 결과 제한 (기본 10)
    
    Response: JSON
        [{ name, schema, description, similarity }, ...]
    """
    user_id = extract_user_id(request)
    api_key = request.headers.get("X-API-Key") or getattr(settings, 'openai_api_key', None)
    
    if not api_key:
        logger.warning("[API] 시멘틱 검색: API 키 없음")
        raise HTTPException(400, {"error": "OpenAI API 키가 필요합니다. 설정에서 API 키를 입력해주세요."})
    
    logger.info("[API] 시멘틱 검색 요청 | user=%s | query=%s", user_id, body.query[:50])
    
    # numpy와 openai 동적 import (설치되지 않은 경우 에러 처리)
    try:
        import numpy as np
        from openai import AsyncOpenAI
    except ImportError as e:
        logger.error("[API] 시멘틱 검색: 필수 라이브러리 미설치 - %s", e)
        raise HTTPException(500, {"error": f"필수 라이브러리가 설치되지 않았습니다: {e}"})
    
    client = Neo4jClient()
    
    try:
        openai_client = AsyncOpenAI(api_key=api_key)
    except Exception as e:
        logger.error("[API] OpenAI 클라이언트 초기화 실패: %s", e)
        raise HTTPException(400, {"error": f"OpenAI API 키가 유효하지 않습니다: {e}"})
    
    try:
        # 1. 테이블 목록 조회 (설명 포함)
        where_conditions = [f"t.user_id = '{escape_for_cypher(user_id)}'"]
        if body.project_name:
            where_conditions.append(f"t.project_name = '{escape_for_cypher(body.project_name)}'")
        
        where_clause = " AND ".join(where_conditions)
        
        cypher_query = f"""
            MATCH (t:Table)
            WHERE {where_clause} AND t.description IS NOT NULL AND t.description <> ''
            RETURN t.name AS name,
                   t.schema AS schema,
                   t.description AS description
            ORDER BY t.name
            LIMIT 200
        """
        
        results = await client.execute_queries([cypher_query])
        records = results[0] if results else []
        
        logger.info("[API] 시멘틱 검색: %d개 테이블 조회됨", len(records))
        
        if not records:
            logger.info("[API] 시멘틱 검색: 설명이 있는 테이블이 없음")
            return []
        
        # 2. 쿼리 임베딩 생성
        try:
            query_response = await openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=body.query
            )
            query_embedding = np.array(query_response.data[0].embedding)
        except Exception as e:
            logger.error("[API] 쿼리 임베딩 생성 실패: %s", e)
            raise HTTPException(500, {"error": f"임베딩 생성 실패: {e}"})
        
        # 3. 테이블 설명 임베딩 생성 (배치 처리)
        descriptions = []
        for r in records:
            desc = r.get("description") or ""
            descriptions.append(desc[:500] if desc else "no description")  # 500자 제한
        
        try:
            desc_response = await openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=descriptions
            )
        except Exception as e:
            logger.error("[API] 설명 임베딩 생성 실패: %s", e)
            raise HTTPException(500, {"error": f"설명 임베딩 생성 실패: {e}"})
        
        # 4. 유사도 계산 (코사인 유사도)
        results_with_similarity = []
        for i, record in enumerate(records):
            desc_embedding = np.array(desc_response.data[i].embedding)
            
            # 코사인 유사도
            norm_product = np.linalg.norm(query_embedding) * np.linalg.norm(desc_embedding)
            if norm_product == 0:
                similarity = 0.0
            else:
                similarity = float(np.dot(query_embedding, desc_embedding) / norm_product)
            
            results_with_similarity.append({
                "name": record["name"],
                "schema": record["schema"] or "public",
                "description": record["description"][:200],  # 응답에서 200자로 제한
                "similarity": round(similarity, 4)
            })
        
        # 5. 유사도 높은 순 정렬 및 제한
        results_with_similarity.sort(key=lambda x: x["similarity"], reverse=True)
        top_results = results_with_similarity[:body.limit]
        
        # 유사도가 0.3 이상인 것만 반환
        filtered_results = [r for r in top_results if r["similarity"] >= 0.3]
        
        logger.info("[API] 시멘틱 검색 완료 | query=%s | results=%d", 
                    body.query[:30], len(filtered_results))
        
        return filtered_results
    
    except Exception as e:
        logger.error("[API] 시멘틱 검색 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/schema/tables", response_model=List[SchemaTableInfo])
async def list_schema_tables(
    request: Request,
    search: Optional[str] = None,
    schema: Optional[str] = None,
    project_name: Optional[str] = None,
    limit: int = 100
):
    """Neo4j에서 테이블 목록 조회 (DDL 분석 결과)
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Query Params:
        search: 테이블명/설명 검색
        schema: 스키마 필터
        project_name: 프로젝트명 필터
        limit: 결과 제한 (기본 100)
    
    Response: JSON
        [{ name, schema, description, column_count, project_name }, ...]
    """
    user_id = extract_user_id(request)
    logger.info("[API] 테이블 목록 조회 | user=%s | project=%s", user_id, project_name)
    
    client = Neo4jClient()
    try:
        # WHERE 조건 생성
        where_conditions = [f"t.user_id = '{escape_for_cypher(user_id)}'"]
        
        if project_name:
            where_conditions.append(f"t.project_name = '{escape_for_cypher(project_name)}'")
        
        if schema:
            where_conditions.append(f"t.schema = '{escape_for_cypher(schema)}'")
        
        if search:
            search_escaped = escape_for_cypher(search)
            where_conditions.append(
                f"(toLower(t.name) CONTAINS toLower('{search_escaped}') "
                f"OR toLower(t.description) CONTAINS toLower('{search_escaped}'))"
            )
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            MATCH (t:Table)
            WHERE {where_clause}
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            WITH t, count(c) AS col_count
            RETURN t.name AS name,
                   t.schema AS schema,
                   t.description AS description,
                   col_count AS column_count,
                   t.project_name AS project_name
            ORDER BY t.name
            LIMIT {limit}
        """
        
        results = await client.execute_queries([query])
        records = results[0] if results else []
        
        tables = [
            SchemaTableInfo(
                name=r["name"],
                table_schema=r["schema"] or "",
                description=r["description"] or "",
                column_count=r["column_count"] or 0,
                project_name=r["project_name"]
            )
            for r in records
        ]
        
        logger.info("[API] 테이블 목록 조회 완료 | count=%d", len(tables))
        return tables
    
    except Exception as e:
        logger.error("[API] 테이블 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/schema/tables/{table_name}/columns", response_model=List[SchemaColumnInfo])
async def list_table_columns(
    request: Request,
    table_name: str,
    schema: str = "",
    project_name: Optional[str] = None
):
    """테이블의 컬럼 목록 조회
    
    Path Params:
        table_name: 테이블명
    
    Query Params:
        schema: 스키마명
        project_name: 프로젝트명
    
    Response: JSON
        [{ name, table_name, dtype, nullable, description }, ...]
    """
    user_id = extract_user_id(request)
    logger.info("[API] 컬럼 목록 조회 | table=%s | schema=%s", table_name, schema)
    
    client = Neo4jClient()
    try:
        where_conditions = [
            f"t.user_id = '{escape_for_cypher(user_id)}'",
            f"t.name = '{escape_for_cypher(table_name)}'"
        ]
        
        if schema:
            where_conditions.append(f"t.schema = '{escape_for_cypher(schema)}'")
        
        if project_name:
            where_conditions.append(f"t.project_name = '{escape_for_cypher(project_name)}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            WHERE {where_clause}
            RETURN c.name AS name,
                   t.name AS table_name,
                   c.dtype AS dtype,
                   c.nullable AS nullable,
                   c.description AS description
            ORDER BY c.name
        """
        
        results = await client.execute_queries([query])
        records = results[0] if results else []
        
        columns = [
            SchemaColumnInfo(
                name=r["name"],
                table_name=r["table_name"],
                dtype=r["dtype"] or "unknown",
                nullable=r.get("nullable", True),
                description=r["description"] or ""
            )
            for r in records
        ]
        
        logger.info("[API] 컬럼 목록 조회 완료 | count=%d", len(columns))
        return columns
    
    except Exception as e:
        logger.error("[API] 컬럼 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/schema/relationships")
async def list_schema_relationships(
    request: Request,
    project_name: Optional[str] = None
):
    """테이블 관계(FK) 목록 조회
    
    Response: JSON
        { relationships: [{ from_table, from_column, to_table, to_column, ... }, ...] }
    """
    user_id = extract_user_id(request)
    logger.info("[API] 관계 목록 조회 | user=%s | project=%s", user_id, project_name)
    
    client = Neo4jClient()
    try:
        where_conditions = [f"t1.user_id = '{escape_for_cypher(user_id)}'"]
        
        if project_name:
            where_conditions.append(f"t1.project_name = '{escape_for_cypher(project_name)}'")
        
        where_clause = " AND ".join(where_conditions)
        
        # FK_TO_TABLE 중 source='user'인 것만 조회 (사용자 추가 관계)
        query = f"""
            MATCH (t1:Table)-[r:FK_TO_TABLE]->(t2:Table)
            WHERE {where_clause} AND r.source = 'user'
            RETURN t1.name AS from_table,
                   t1.schema AS from_schema,
                   r.sourceColumn AS from_column,
                   t2.name AS to_table,
                   t2.schema AS to_schema,
                   r.targetColumn AS to_column,
                   r.type AS relationship_type,
                   r.description AS description
            ORDER BY from_table, to_table
        """
        
        results = await client.execute_queries([query])
        records = results[0] if results else []
        
        relationships = [
            {
                "from_table": r["from_table"],
                "from_schema": r["from_schema"] or "",
                "from_column": r["from_column"] or "",
                "to_table": r["to_table"],
                "to_schema": r["to_schema"] or "",
                "to_column": r["to_column"] or "",
                "relationship_type": r["relationship_type"] or "many_to_one",
                "description": r["description"] or ""
            }
            for r in records
        ]
        
        logger.info("[API] 관계 목록 조회 완료 | count=%d", len(relationships))
        return {"relationships": relationships}
    
    except Exception as e:
        logger.error("[API] 관계 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.post("/schema/relationships")
async def add_schema_relationship(
    request: Request,
    body: AddRelationshipRequest
):
    """테이블 관계 추가 (ERD 모델링)
    
    Request Body:
        from_table, from_column, to_table, to_column, relationship_type, description
    
    Response: JSON
        { success: true, message: ... }
    """
    user_id = extract_user_id(request)
    logger.info(
        "[API] 관계 추가 | user=%s | %s.%s -> %s.%s",
        user_id, body.from_table, body.from_column, body.to_table, body.to_column
    )
    
    client = Neo4jClient()
    try:
        # FK_TO_TABLE 관계 생성 (source: 'user' - 사용자 추가)
        query = f"""
            MATCH (t1:Table {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(body.from_table)}'}})
            MATCH (t2:Table {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(body.to_table)}'}})
            MERGE (t1)-[r:FK_TO_TABLE {{
                sourceColumn: '{escape_for_cypher(body.from_column)}',
                targetColumn: '{escape_for_cypher(body.to_column)}'
            }}]->(t2)
            SET r.type = '{escape_for_cypher(body.relationship_type)}',
                r.source = 'user',
                r.description = '{escape_for_cypher(body.description)}',
                r.created_at = datetime()
            RETURN count(r) AS count
        """
        
        await client.execute_queries([query])
        
        logger.info("[API] 관계 추가 완료")
        return {"success": True, "message": "관계가 추가되었습니다."}
    
    except Exception as e:
        logger.error("[API] 관계 추가 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.delete("/schema/relationships")
async def delete_schema_relationship(
    request: Request,
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str
):
    """테이블 관계 삭제
    
    Query Params:
        from_table, from_column, to_table, to_column
    
    Response: JSON
        { success: true, message: ... }
    """
    user_id = extract_user_id(request)
    logger.info(
        "[API] 관계 삭제 | user=%s | %s.%s -> %s.%s",
        user_id, from_table, from_column, to_table, to_column
    )
    
    client = Neo4jClient()
    try:
        # FK_TO_TABLE 중 source='user'인 관계만 삭제 (사용자 추가 관계)
        query = f"""
            MATCH (t1:Table {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(from_table)}'}})-
                  [r:FK_TO_TABLE {{sourceColumn: '{escape_for_cypher(from_column)}', targetColumn: '{escape_for_cypher(to_column)}', source: 'user'}}]->
                  (t2:Table {{name: '{escape_for_cypher(to_table)}'}})
            DELETE r
            RETURN count(r) AS count
        """
        
        await client.execute_queries([query])
        
        logger.info("[API] 관계 삭제 완료")
        return {"success": True, "message": "관계가 삭제되었습니다."}
    
    except Exception as e:
        logger.error("[API] 관계 삭제 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


# =============================================================================
# 파이프라인 제어 API
# =============================================================================

from analyzer.pipeline_control import pipeline_controller, PipelineAction


class PipelineControlRequest(BaseModel):
    """파이프라인 제어 요청"""
    action: str  # "pause" | "resume" | "stop"


@router.get("/pipeline/status")
async def get_pipeline_status(request: Request):
    """파이프라인 상태 조회
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
        { sessionId, currentPhase, phaseName, isPaused, isStopped, phaseProgress, phases: [...] }
    """
    session_id = extract_user_id(request)
    return pipeline_controller.get_status(session_id)


@router.get("/pipeline/phases")
async def get_pipeline_phases():
    """파이프라인 단계 정보 조회
    
    Response: JSON
        [{ phase, name, description, order, canPause }, ...]
    """
    return pipeline_controller.get_phases_info()


@router.post("/pipeline/control")
async def control_pipeline(request: Request, body: PipelineControlRequest):
    """파이프라인 제어 (일시정지/재개/중단)
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Request Body:
        action: "pause" | "resume" | "stop"
    
    Response: JSON
        { success, action, status: { ... } }
    """
    session_id = extract_user_id(request)
    action = body.action.lower()
    
    logger.info("[API] 파이프라인 제어 | session=%s | action=%s", session_id, action)
    
    success = False
    if action == "pause":
        success = pipeline_controller.pause(session_id)
    elif action == "resume":
        success = pipeline_controller.resume(session_id)
    elif action == "stop":
        success = pipeline_controller.stop(session_id)
    else:
        raise HTTPException(400, f"알 수 없는 액션: {action}")
    
    return {
        "success": success,
        "action": action,
        "status": pipeline_controller.get_status(session_id)
    }
