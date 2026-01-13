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
        OpenAI-Api-Key: LLM API 키 (필수)
        Accept-Language: 출력 언어 (기본: ko)
    
    Request Body:
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
        "[API] 분석 시작 | strategy=%s | files=%d | has_ddl=%s",
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
    
    Response: JSON
        hasData: bool - 기존 데이터 존재 여부
        nodeCount: int - 노드 개수
    """
    logger.info("[API] 데이터 존재 확인 요청")

    client = Neo4jClient()
    try:
        result = await client.execute_queries([
            "MATCH (n) RETURN count(n) as count"
        ])
        node_count = result[0][0]["count"] if result and result[0] else 0
        
        return {
            "hasData": node_count > 0,
            "nodeCount": node_count
        }
    except Exception as e:
        logger.error("[API] 데이터 확인 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/graph/")
async def get_graph_data(request: Request):
    """Neo4j에서 기존 그래프 데이터 조회
    
    Response: JSON
        Nodes: [{ "Node ID", "Labels", "Properties" }, ...]
        Relationships: [{ "Relationship ID", "Start Node ID", "End Node ID", "Type", "Properties" }, ...]
    """
    logger.info("[API] 그래프 데이터 조회")

    client = Neo4jClient()
    try:
        # 노드 조회 (전체 노드)
        node_query = """
            MATCH (n)
            RETURN elementId(n) AS nodeId, labels(n) AS labels, properties(n) AS props
        """
        
        # 관계 조회 (전체 관계)
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
        logger.error("[API] 그래프 조회 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/graph/related-tables/{table_name}")
async def get_related_tables(table_name: str, request: Request):
    """특정 테이블과 연결된 모든 테이블 조회 (FK_TO_TABLE 관계 포함)
    
    Path Params:
        table_name: 기준 테이블명
    
    Response: JSON
        tables: [{ name, schema, description }, ...]
        relationships: [{ from_table, to_table, type, column_pairs: [{source, target}] }, ...]
    """
    logger.info("[API] 관련 테이블 조회 요청 | table=%s", table_name)
    
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
    Response: JSON
    """
    logger.info("[API] 데이터 삭제 요청 | include_files=%s", include_files)

    try:
        orchestrator = AnalysisOrchestrator(
            api_key="",
            locale="",
        )
        
        if include_files:
            await orchestrator.cleanup_all_data(include_files=True)
            logger.info("[API] 전체 데이터 삭제 완료 (파일 포함)")
            return {"message": "모든 데이터(파일 + Neo4j)가 삭제되었습니다."}
        else:
            await orchestrator.cleanup_neo4j_data()
            logger.info("[API] Neo4j 데이터만 삭제 완료")
            return {"message": "Neo4j 그래프 데이터가 삭제되었습니다. (파일은 유지됨)"}
    except Exception as e:
        logger.error("[API] 데이터 삭제 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))


# =============================================================================
# 데이터 리니지 API
# =============================================================================

class LineageAnalyzeRequest(BaseModel):
    """데이터 리니지 분석 요청"""
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
async def get_lineage_graph():
    """데이터 리니지 그래프 조회
    
    Response: JSON
        nodes: [{ id, name, type, properties }, ...]
        edges: [{ id, source, target, type, properties }, ...]
        stats: { etlCount, sourceCount, targetCount, flowCount }
    """
    logger.info("[API] 리니지 조회 요청")
    
    client = Neo4jClient()
    try:
        # DataSource, ETLProcess 노드 조회
        node_query = """
            MATCH (n)
            WHERE (n:DataSource OR n:ETLProcess)
            RETURN n.name AS name,
                   labels(n)[0] AS nodeType,
                   elementId(n) AS id,
                   properties(n) AS properties
            ORDER BY nodeType, name
        """
        
        # 관계 조회 쿼리
        rel_query = """
            MATCH (src)-[r]->(tgt)
            WHERE (src:DataSource OR src:ETLProcess)
              AND (tgt:DataSource OR tgt:ETLProcess)
              AND type(r) IN ['DATA_FLOW_TO', 'TRANSFORMS_TO']
            RETURN elementId(r) AS id,
                   elementId(src) AS source,
                   elementId(tgt) AS target,
                   type(r) AS relType,
                   properties(r) AS properties
        """
        
        # 통계 쿼리
        stats_query = """
            MATCH (n)
            WHERE (n:DataSource OR n:ETLProcess)
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
        logger.error("[API] 리니지 조회 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.post("/lineage/analyze/")
async def analyze_lineage(request: Request, body: LineageAnalyzeRequest):
    """ETL 코드에서 데이터 리니지 추출 및 Neo4j 저장
    
    Request Body:
        sqlContent: SQL 소스 코드 (필수)
        fileName: 파일명 (선택)
        dbms: DBMS 타입 (기본: oracle)
    
    Response: JSON
        lineages: [{ etl_name, source_tables, target_tables, operation_type }, ...]
        stats: { etl_nodes, data_sources, data_flows }
    """
    logger.info("[API] 리니지 분석 요청 | file=%s", body.fileName)
    
    try:
        lineage_list, stats = await analyze_lineage_from_sql(
            sql_content=body.sqlContent,
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
        logger.error("[API] 리니지 분석 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))


# =============================================================================
# 스키마 API (ERD 모델링용)
# =============================================================================

class SchemaTableInfo(BaseModel):
    """테이블 정보"""
    name: str
    table_schema: str  # Renamed from 'schema' to avoid BaseModel attribute conflict
    description: str
    description_source: Optional[str] = ""  # 설명 출처: ddl, procedure, user
    analyzed_description: Optional[str] = ""  # 프로시저 분석에서 도출된 설명
    column_count: int


class SchemaColumnInfo(BaseModel):
    """컬럼 정보"""
    name: str
    table_name: str
    dtype: str
    nullable: bool
    description: str
    description_source: Optional[str] = ""  # 설명 출처: ddl, procedure, user
    analyzed_description: Optional[str] = ""  # 프로시저 분석에서 도출된 설명


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
        X-API-Key: OpenAI API 키 (선택, 없으면 환경변수 사용)
    
    Request Body:
        query: 검색 쿼리
        limit: 결과 제한 (기본 10)
    
    Response: JSON
        [{ name, schema, description, similarity }, ...]
    """
    api_key = request.headers.get("X-API-Key") or settings.llm.api_key
    
    if not api_key:
        logger.warning("[API] 시멘틱 검색: API 키 없음")
        raise HTTPException(400, {"error": "OpenAI API 키가 필요합니다. 설정에서 API 키를 입력해주세요."})
    
    logger.info("[API] 시멘틱 검색 요청 | query=%s", body.query[:50])
    
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
        cypher_query = """
            MATCH (t:Table)
            WHERE t.description IS NOT NULL AND t.description <> ''
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
        logger.error("[API] 시멘틱 검색 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.get("/schema/tables", response_model=List[SchemaTableInfo])
async def list_schema_tables(
    request: Request,
    search: Optional[str] = None,
    schema: Optional[str] = None,
    limit: int = 100
):
    """Neo4j에서 테이블 목록 조회 (DDL 분석 결과)
    
    Query Params:
        search: 테이블명/설명 검색
        schema: 스키마 필터
        limit: 결과 제한 (기본 100)
    
    Response: JSON
        [{ name, schema, description, column_count }, ...]
    """
    logger.info("[API] 테이블 목록 조회")
    
    client = Neo4jClient()
    try:
        # WHERE 조건 생성
        where_conditions = []
        
        if schema:
            where_conditions.append(f"t.schema = '{escape_for_cypher(schema)}'")
        
        if search:
            search_escaped = escape_for_cypher(search)
            where_conditions.append(
                f"(toLower(t.name) CONTAINS toLower('{search_escaped}') "
                f"OR toLower(t.description) CONTAINS toLower('{search_escaped}'))"
            )
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "true"
        
        query = f"""
            MATCH (t:Table)
            WHERE {where_clause}
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            WITH t, count(c) AS col_count
            RETURN t.name AS name,
                   t.schema AS schema,
                   t.description AS description,
                   t.description_source AS description_source,
                   t.analyzed_description AS analyzed_description,
                   col_count AS column_count
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
                description_source=r["description_source"] or "",
                analyzed_description=r["analyzed_description"] or "",
                column_count=r["column_count"] or 0
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
    schema: str = ""
):
    """테이블의 컬럼 목록 조회
    
    Path Params:
        table_name: 테이블명
    
    Query Params:
        schema: 스키마명
    
    Response: JSON
        [{ name, table_name, dtype, nullable, description }, ...]
    """
    logger.info("[API] 컬럼 목록 조회 | table=%s | schema=%s", table_name, schema)
    
    client = Neo4jClient()
    try:
        where_conditions = [
            f"t.name = '{escape_for_cypher(table_name)}'"
        ]
        
        if schema:
            where_conditions.append(f"t.schema = '{escape_for_cypher(schema)}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            WHERE {where_clause}
            RETURN c.name AS name,
                   t.name AS table_name,
                   c.dtype AS dtype,
                   c.nullable AS nullable,
                   c.description AS description,
                   c.description_source AS description_source,
                   c.analyzed_description AS analyzed_description
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
                description=r["description"] or "",
                description_source=r.get("description_source") or "",
                analyzed_description=r.get("analyzed_description") or ""
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
    request: Request
):
    """테이블 관계(FK) 목록 조회
    
    Response: JSON
        { relationships: [{ from_table, from_column, to_table, to_column, ... }, ...] }
    """
    logger.info("[API] 관계 목록 조회")
    
    client = Neo4jClient()
    try:
        # FK_TO_TABLE 중 source='user'인 것만 조회 (사용자 추가 관계)
        query = """
            MATCH (t1:Table)-[r:FK_TO_TABLE]->(t2:Table)
            WHERE r.source = 'user'
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


class ProcedureReferenceInfo(BaseModel):
    """프로시저 참조 정보"""
    procedure_name: str
    procedure_type: str  # PROCEDURE, FUNCTION 등
    start_line: int
    access_type: str  # FROM (읽기), WRITES (쓰기)
    statement_type: Optional[str] = None  # SELECT, INSERT, UPDATE 등
    statement_line: Optional[int] = None
    file_name: Optional[str] = None  # 파일명
    file_directory: Optional[str] = None  # 파일 경로


@router.get("/schema/tables/{table_name}/references")
async def get_table_references(
    request: Request,
    table_name: str,
    schema: str = "",
    column_name: Optional[str] = None
):
    """테이블 또는 컬럼이 참조된 프로시저 목록 조회
    
    Path Params:
        table_name: 테이블명
        
    Query Params:
        schema: 스키마명 (선택)
        column_name: 컬럼명 (선택 - 특정 컬럼 참조 조회 시)
    
    Response: JSON
        { references: [{ procedure_name, procedure_type, start_line, access_type, statement_type, statement_line }, ...] }
    """
    logger.info("[API] 테이블 참조 조회 | table=%s | schema=%s | column=%s", table_name, schema, column_name)
    
    client = Neo4jClient()
    try:
        # 테이블 조건
        table_conditions = [
            f"t.name = '{escape_for_cypher(table_name)}'"
        ]
        if schema:
            table_conditions.append(f"t.schema = '{escape_for_cypher(schema)}'")
        
        table_where = " AND ".join(table_conditions)
        
        # Statement -> Table 관계 (FROM, WRITES)를 통해 프로시저 탐색
        # Statement는 PROCEDURE 또는 FUNCTION의 하위 노드 (PARENT_OF 관계)
        # FILE -> PROCEDURE 관계 (CONTAINS)를 통해 파일 정보도 함께 조회
        query = f"""
            MATCH (t:Table)
            WHERE {table_where}
            MATCH (s)-[rel:FROM|WRITES]->(t)
            OPTIONAL MATCH (s)<-[:PARENT_OF*]-(proc)
            WHERE proc:PROCEDURE OR proc:FUNCTION
            OPTIONAL MATCH (file:FILE)-[:CONTAINS]->(proc)
            RETURN DISTINCT 
                COALESCE(proc.name, s.name) AS procedure_name,
                COALESCE(labels(proc)[0], labels(s)[0]) AS procedure_type,
                COALESCE(proc.startLine, s.startLine) AS start_line,
                type(rel) AS access_type,
                labels(s)[0] AS statement_type,
                s.startLine AS statement_line,
                file.file_name AS file_name,
                file.directory AS file_directory
            ORDER BY procedure_name, statement_line
        """
        
        results = await client.execute_queries([query])
        records = results[0] if results else []
        
        references = [
            ProcedureReferenceInfo(
                procedure_name=r["procedure_name"] or "",
                procedure_type=r["procedure_type"] or "",
                start_line=r["start_line"] or 0,
                access_type=r["access_type"] or "",
                statement_type=r["statement_type"],
                statement_line=r["statement_line"],
                file_name=r.get("file_name"),
                file_directory=r.get("file_directory")
            )
            for r in records
        ]
        
        logger.info("[API] 테이블 참조 조회 완료 | table=%s | count=%d", table_name, len(references))
        return {"references": references}
    
    except Exception as e:
        logger.error("[API] 테이블 참조 조회 실패 | error=%s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


class StatementSummaryInfo(BaseModel):
    """Statement 요약 정보"""
    start_line: int
    end_line: Optional[int] = None
    statement_type: str
    summary: Optional[str] = None
    ai_description: Optional[str] = None


@router.get("/schema/procedures/{procedure_name}/statements")
async def get_procedure_statements(
    request: Request,
    procedure_name: str,
    file_directory: Optional[str] = None
):
    """프로시저의 모든 Statement와 AI 설명 조회
    
    Path Params:
        procedure_name: 프로시저명
        
    Query Params:
        file_directory: 파일 경로 (선택)
    
    Response: JSON
        { statements: [{ start_line, end_line, statement_type, summary, ai_description }, ...] }
    """
    logger.info("[API] 프로시저 Statement 조회 | proc=%s | file=%s", procedure_name, file_directory)
    
    client = Neo4jClient()
    try:
        # 프로시저 조건
        proc_conditions = [
            f"proc.name = '{escape_for_cypher(procedure_name)}'"
        ]
        if file_directory:
            proc_conditions.append(f"proc.directory = '{escape_for_cypher(file_directory)}'")
        
        proc_where = " AND ".join(proc_conditions)
        
        # 프로시저의 모든 하위 노드(Statement)와 그 summary 조회
        query = f"""
            MATCH (proc)
            WHERE (proc:PROCEDURE OR proc:FUNCTION) AND {proc_where}
            OPTIONAL MATCH (proc)-[:PARENT_OF*]->(s)
            RETURN DISTINCT
                s.startLine AS start_line,
                s.endLine AS end_line,
                labels(s)[0] AS statement_type,
                s.summary AS summary,
                s.ai_description AS ai_description
            ORDER BY s.startLine
        """
        
        results = await client.execute_queries([query])
        records = results[0] if results else []
        
        statements = [
            StatementSummaryInfo(
                start_line=r["start_line"] or 0,
                end_line=r.get("end_line"),
                statement_type=r["statement_type"] or "",
                summary=r.get("summary"),
                ai_description=r.get("ai_description")
            )
            for r in records
            if r["start_line"]  # start_line이 있는 것만
        ]
        
        logger.info("[API] 프로시저 Statement 조회 완료 | proc=%s | count=%d", procedure_name, len(statements))
        return {"statements": statements}
    
    except Exception as e:
        logger.error("[API] 프로시저 Statement 조회 실패 | error=%s", e)
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
    logger.info(
        "[API] 관계 추가 | %s.%s -> %s.%s",
        body.from_table, body.from_column, body.to_table, body.to_column
    )
    
    client = Neo4jClient()
    try:
        # FK_TO_TABLE 관계 생성 (source: 'user' - 사용자 추가)
        query = f"""
            MATCH (t1:Table {{name: '{escape_for_cypher(body.from_table)}'}})
            MATCH (t2:Table {{name: '{escape_for_cypher(body.to_table)}'}})
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
    logger.info(
        "[API] 관계 삭제 | %s.%s -> %s.%s",
        from_table, from_column, to_table, to_column
    )
    
    client = Neo4jClient()
    try:
        # FK_TO_TABLE 중 source='user'인 관계만 삭제 (사용자 추가 관계)
        query = f"""
            MATCH (t1:Table {{name: '{escape_for_cypher(from_table)}'}})-
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
async def get_pipeline_status():
    """파이프라인 상태 조회
    
    Response: JSON
        { currentPhase, phaseName, isPaused, isStopped, phaseProgress, phases: [...] }
    """
    return pipeline_controller.get_status()


@router.get("/pipeline/phases")
async def get_pipeline_phases():
    """파이프라인 단계 정보 조회
    
    Response: JSON
        [{ phase, name, description, order, canPause }, ...]
    """
    return pipeline_controller.get_phases_info()


@router.post("/pipeline/control")
async def control_pipeline(body: PipelineControlRequest):
    """파이프라인 제어 (일시정지/재개/중단)
    
    Request Body:
        action: "pause" | "resume" | "stop"
    
    Response: JSON
        { success, action, status: { ... } }
    """
    action = body.action.lower()
    
    logger.info("[API] 파이프라인 제어 | action=%s", action)
    
    success = False
    if action == "pause":
        success = pipeline_controller.pause()
    elif action == "resume":
        success = pipeline_controller.resume()
    elif action == "stop":
        success = pipeline_controller.stop()
    else:
        raise HTTPException(400, f"알 수 없는 액션: {action}")
    
    return {
        "success": success,
        "action": action,
        "status": pipeline_controller.get_status()
    }


# =============================================================================
# Schema Edit API - 테이블/컬럼 설명 편집 및 벡터라이징
# =============================================================================

class TableDescriptionUpdateRequest(BaseModel):
    """테이블 설명 업데이트 요청"""
    name: str
    schema: str = "public"
    description: Optional[str] = None


class ColumnDescriptionUpdateRequest(BaseModel):
    """컬럼 설명 업데이트 요청"""
    table_name: str
    table_schema: str = "public"
    column_name: str
    description: Optional[str] = None


class VectorizeRequest(BaseModel):
    """벡터라이징 요청"""
    db_name: Optional[str] = "postgres"
    schema: Optional[str] = None
    include_tables: bool = True
    include_columns: bool = True
    reembed_existing: bool = False
    batch_size: int = 100


@router.put("/schema/tables/{table_name}/description")
async def update_table_description(
    table_name: str,
    request: Request,
    body: TableDescriptionUpdateRequest
):
    """테이블 설명 업데이트 및 임베딩 재생성
    
    Request Body:
        name, schema, description
    
    Response:
        { message, data: { name, description }, embedding_updated: bool }
    """
    from openai import AsyncOpenAI
    from util.embedding_client import EmbeddingClient
    
    api_key = request.headers.get("X-API-Key") or settings.llm.api_key
    
    logger.info("[API] 테이블 설명 업데이트 | table=%s.%s", body.schema, table_name)
    
    client = Neo4jClient()
    try:
        # 1. 테이블 정보 조회 (컬럼 목록 포함)
        info_query = f"""
            MATCH (t:Table {{name: '{escape_for_cypher(table_name)}', schema: '{escape_for_cypher(body.schema)}'}})
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            RETURN t.name AS name, t.schema AS schema, collect(c.name) AS columns
        """
        
        results = await client.execute_queries([info_query])
        records = results[0] if results else []
        
        if not records:
            raise HTTPException(404, "테이블을 찾을 수 없습니다")
        
        table_info = records[0]
        columns = table_info.get("columns", [])
        
        # 2. 임베딩 생성 (설명 + 컬럼 목록 포함)
        embedding = None
        if api_key and body.description:
            try:
                openai_client = AsyncOpenAI(api_key=api_key)
                embedding_client = EmbeddingClient(openai_client)
                embed_text = embedding_client.format_table_text(
                    table_name=table_name,
                    description=body.description or "",
                    columns=columns
                )
                embedding = await embedding_client.embed_text(embed_text)
                logger.info("[API] 테이블 '%s' 임베딩 재생성 완료 (dim=%d)", table_name, len(embedding))
            except Exception as e:
                logger.warning("[API] 테이블 '%s' 임베딩 생성 실패: %s", table_name, e)
        
        # 3. 설명 및 벡터 업데이트
        escaped_desc = escape_for_cypher(body.description or "")
        if embedding:
            update_query = f"""
                MATCH (t:Table {{name: '{escape_for_cypher(table_name)}', schema: '{escape_for_cypher(body.schema)}'}})
                SET t.description = '{escaped_desc}', t.description_source = 'user', t.vector = {embedding}, t.updated_at = datetime()
                RETURN t.name AS name, t.description AS description, t.description_source AS description_source
            """
        else:
            update_query = f"""
                MATCH (t:Table {{name: '{escape_for_cypher(table_name)}', schema: '{escape_for_cypher(body.schema)}'}})
                SET t.description = '{escaped_desc}', t.description_source = 'user'
                RETURN t.name AS name, t.description AS description, t.description_source AS description_source
            """
        
        results = await client.execute_queries([update_query])
        records = results[0] if results else []
        
        if not records:
            raise HTTPException(404, "테이블을 찾을 수 없습니다")
        
        return {
            "message": "테이블 설명 업데이트 완료" + (" (임베딩 포함)" if embedding else ""),
            "data": records[0],
            "embedding_updated": embedding is not None
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 테이블 설명 업데이트 실패: %s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.put("/schema/tables/{table_name}/columns/{column_name}/description")
async def update_column_description(
    table_name: str,
    column_name: str,
    request: Request,
    body: ColumnDescriptionUpdateRequest
):
    """컬럼 설명 업데이트 및 임베딩 재생성
    
    Request Body:
        table_name, table_schema, column_name, description
    
    Response:
        { message, data: { name, description }, embedding_updated: bool }
    """
    from openai import AsyncOpenAI
    from util.embedding_client import EmbeddingClient
    
    api_key = request.headers.get("X-API-Key") or settings.llm.api_key
    
    logger.info("[API] 컬럼 설명 업데이트 | column=%s.%s.%s", body.table_schema, table_name, column_name)
    
    client = Neo4jClient()
    try:
        # 1. 컬럼 정보 조회
        info_query = f"""
            MATCH (t:Table {{name: '{escape_for_cypher(table_name)}', schema: '{escape_for_cypher(body.table_schema)}'}})-[:HAS_COLUMN]->(c:Column {{name: '{escape_for_cypher(column_name)}'}})
            RETURN c.name AS name, c.dtype AS dtype
        """
        
        results = await client.execute_queries([info_query])
        records = results[0] if results else []
        
        if not records:
            raise HTTPException(404, "컬럼을 찾을 수 없습니다")
        
        col_info = records[0]
        dtype = col_info.get("dtype", "unknown")
        
        # 2. 임베딩 생성
        embedding = None
        if api_key and body.description:
            try:
                openai_client = AsyncOpenAI(api_key=api_key)
                embedding_client = EmbeddingClient(openai_client)
                embed_text = embedding_client.format_column_text(
                    column_name=column_name,
                    table_name=table_name,
                    dtype=dtype or "",
                    description=body.description or ""
                )
                embedding = await embedding_client.embed_text(embed_text)
                logger.info("[API] 컬럼 '%s.%s' 임베딩 재생성 완료 (dim=%d)", table_name, column_name, len(embedding))
            except Exception as e:
                logger.warning("[API] 컬럼 '%s.%s' 임베딩 생성 실패: %s", table_name, column_name, e)
        
        # 3. 설명 및 벡터 업데이트
        escaped_desc = escape_for_cypher(body.description or "")
        if embedding:
            update_query = f"""
                MATCH (t:Table {{name: '{escape_for_cypher(table_name)}', schema: '{escape_for_cypher(body.table_schema)}'}})-[:HAS_COLUMN]->(c:Column {{name: '{escape_for_cypher(column_name)}'}})
                SET c.description = '{escaped_desc}', c.description_source = 'user', c.vector = {embedding}, c.updated_at = datetime()
                RETURN c.name AS name, c.description AS description, c.description_source AS description_source
            """
        else:
            update_query = f"""
                MATCH (t:Table {{name: '{escape_for_cypher(table_name)}', schema: '{escape_for_cypher(body.table_schema)}'}})-[:HAS_COLUMN]->(c:Column {{name: '{escape_for_cypher(column_name)}'}})
                SET c.description = '{escaped_desc}', c.description_source = 'user'
                RETURN c.name AS name, c.description AS description, c.description_source AS description_source
            """
        
        results = await client.execute_queries([update_query])
        records = results[0] if results else []
        
        if not records:
            raise HTTPException(404, "컬럼을 찾을 수 없습니다")
        
        return {
            "message": "컬럼 설명 업데이트 완료" + (" (임베딩 포함)" if embedding else ""),
            "data": records[0],
            "embedding_updated": embedding is not None
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 컬럼 설명 업데이트 실패: %s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.post("/schema/vectorize")
async def vectorize_schema(
    request: Request,
    body: VectorizeRequest
):
    """테이블/컬럼 벡터라이징 (임베딩 생성)
    
    기존 Neo4j 그래프의 테이블/컬럼 description을 기반으로 임베딩 생성
    
    Request Body:
        db_name, schema, include_tables, include_columns, reembed_existing, batch_size
    
    Response:
        { message, status, tables_vectorized, columns_vectorized }
    """
    from openai import AsyncOpenAI
    from util.embedding_client import EmbeddingClient
    
    api_key = request.headers.get("X-API-Key") or settings.llm.api_key
    
    if not api_key:
        raise HTTPException(400, "OpenAI API 키가 필요합니다")
    
    logger.info("[API] 벡터라이징 시작 | schema=%s", body.schema)
    
    openai_client = AsyncOpenAI(api_key=api_key)
    embedding_client = EmbeddingClient(openai_client)
    client = Neo4jClient()
    
    total_tables = 0
    total_columns = 0
    
    try:
        # 테이블 벡터라이징
        if body.include_tables:
            where_parts = []
            if body.schema:
                where_parts.append(f"toLower(t.schema) = toLower('{escape_for_cypher(body.schema)}')")
            if not body.reembed_existing:
                where_parts.append("(t.vector IS NULL OR size(t.vector) = 0)")
            where_parts.append("(t.description IS NOT NULL OR t.analyzed_description IS NOT NULL)")
            
            where_clause = " AND ".join(where_parts) if where_parts else "true"
            
            table_query = f"""
                MATCH (t:Table)
                WHERE {where_clause}
                RETURN elementId(t) AS tid, t.name AS name, t.schema AS schema,
                       coalesce(t.description, t.analyzed_description, '') AS description
                ORDER BY t.schema, t.name
            """
            
            results = await client.execute_queries([table_query])
            tables = results[0] if results else []
            
            for item in tables:
                description = item.get("description", "") or ""
                if not description:
                    continue
                
                text = embedding_client.format_table_text(
                    table_name=item.get("name", ""),
                    description=description
                )
                vector = await embedding_client.embed_text(text)
                
                if vector:
                    set_query = f"""
                        MATCH (t)
                        WHERE elementId(t) = '{item['tid']}'
                        SET t.vector = {vector}, t.updated_at = datetime()
                    """
                    await client.execute_queries([set_query])
                    total_tables += 1
        
        # 컬럼 벡터라이징
        if body.include_columns:
            where_parts = []
            if body.schema:
                where_parts.append(f"toLower(t.schema) = toLower('{escape_for_cypher(body.schema)}')")
            if not body.reembed_existing:
                where_parts.append("(c.embedding IS NULL OR size(c.embedding) = 0)")
            where_parts.append("c.description IS NOT NULL AND c.description <> ''")
            
            where_clause = " AND ".join(where_parts) if where_parts else "true"
            
            column_query = f"""
                MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                WHERE {where_clause}
                RETURN elementId(c) AS cid, c.name AS column_name, t.name AS table_name,
                       coalesce(c.dtype, '') AS dtype, c.description AS description
                ORDER BY t.schema, t.name, c.name
            """
            
            results = await client.execute_queries([column_query])
            columns = results[0] if results else []
            
            batch_size = max(1, body.batch_size)
            for i in range(0, len(columns), batch_size):
                batch = columns[i:i + batch_size]
                texts = []
                
                for item in batch:
                    text = embedding_client.format_column_text(
                        column_name=item.get("column_name", ""),
                        table_name=item.get("table_name", ""),
                        dtype=item.get("dtype", ""),
                        description=item.get("description", "")
                    )
                    texts.append(text)
                
                vectors = await embedding_client.embed_batch(texts)
                
                for item, vector in zip(batch, vectors):
                    if vector:
                        set_query = f"""
                            MATCH (c)
                            WHERE elementId(c) = '{item['cid']}'
                            SET c.vector = {vector}, c.updated_at = datetime()
                        """
                        await client.execute_queries([set_query])
                        total_columns += 1
        
        logger.info("[API] 벡터라이징 완료 | tables=%d | columns=%d", total_tables, total_columns)
        
        return {
            "message": "벡터라이징 완료",
            "status": "success",
            "tables_vectorized": total_tables,
            "columns_vectorized": total_columns
        }
    
    except Exception as e:
        logger.error("[API] 벡터라이징 실패: %s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


# =============================================================================
# DW Star Schema Registration API - OLAP 스타스키마 등록 + 벡터라이징
# =============================================================================

class DWColumnInfo(BaseModel):
    """DW 컬럼 정보"""
    name: str
    dtype: str = "VARCHAR"
    description: Optional[str] = None
    is_pk: bool = False
    is_fk: bool = False
    fk_target_table: Optional[str] = None  # FK 대상 테이블 (예: "dw.dim_time")


class DWDimensionInfo(BaseModel):
    """DW 디멘전 테이블 정보"""
    name: str  # 테이블명 (예: "dim_time")
    columns: List[DWColumnInfo] = []
    source_tables: List[str] = []  # 원본 테이블 (예: ["RWIS.RDF01HH_TB"])


class DWFactTableInfo(BaseModel):
    """DW 팩트 테이블 정보"""
    name: str  # 테이블명 (예: "fact_turbidity")
    columns: List[DWColumnInfo] = []
    source_tables: List[str] = []


class DWStarSchemaRequest(BaseModel):
    """DW 스타스키마 등록 요청"""
    cube_name: str
    db_name: str = "postgres"
    dw_schema: str = "dw"
    fact_table: DWFactTableInfo
    dimensions: List[DWDimensionInfo] = []
    create_embeddings: bool = True  # 임베딩 자동 생성 여부


@router.post("/schema/dw-tables")
async def register_dw_star_schema(
    request: Request,
    body: DWStarSchemaRequest
):
    """DW 스타스키마 테이블/컬럼을 Neo4j에 등록하고 벡터 임베딩 생성
    
    OLAP 백엔드에서 호출하여 DW 테이블을 Neo4j에 등록합니다.
    기존 robo-analyzer의 벡터라이징 기능을 활용합니다.
    
    Request Body:
        cube_name: 큐브 이름
        db_name: 데이터베이스 이름
        dw_schema: DW 스키마명 (기본: dw)
        fact_table: 팩트 테이블 정보
        dimensions: 디멘전 테이블 목록
        create_embeddings: 임베딩 자동 생성 여부
    
    Response:
        { success, message, tables_created, columns_created, embeddings_created }
    """
    from openai import AsyncOpenAI
    from util.embedding_client import EmbeddingClient
    
    api_key = request.headers.get("X-API-Key") or settings.llm.api_key
    
    logger.info("[API] DW 스타스키마 등록 시작 | cube=%s | schema=%s", body.cube_name, body.dw_schema)
    
    client = Neo4jClient()
    tables_created = 0
    columns_created = 0
    embeddings_created = 0
    
    try:
        queries = []
        
        # 1. Schema 노드 생성/업데이트
        queries.append(f"""
            MERGE (s:Schema {{db: '{escape_for_cypher(body.db_name)}', name: '{escape_for_cypher(body.dw_schema)}'}})
            SET s.description = 'Data Warehouse schema for OLAP cubes',
                s.type = 'DW',
                s.updated_at = datetime()
            RETURN s
        """)
        
        # 2. 디멘전 테이블 생성
        for dim in body.dimensions:
            dim_table = dim.name
            dim_desc = f"Dimension table for {body.cube_name} cube"
            
            # 테이블 노드 생성
            queries.append(f"""
                MERGE (t:Table {{
                    db: '{escape_for_cypher(body.db_name)}',
                    schema: '{escape_for_cypher(body.dw_schema)}',
                    name: '{escape_for_cypher(dim_table)}'
                }})
                SET t.table_type = 'DIMENSION',
                    t.cube_name = '{escape_for_cypher(body.cube_name)}',
                    t.description = '{escape_for_cypher(dim_desc)}',
                    t.updated_at = datetime()
                RETURN t.name AS name
            """)
            tables_created += 1
            
            # Schema -> Table 관계
            queries.append(f"""
                MATCH (s:Schema {{db: '{escape_for_cypher(body.db_name)}', name: '{escape_for_cypher(body.dw_schema)}'}})
                MATCH (t:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(body.dw_schema)}', name: '{escape_for_cypher(dim_table)}'}})
                MERGE (s)-[:HAS_TABLE]->(t)
            """)
            
            # 컬럼 노드 생성
            for col in dim.columns:
                col_fqn = f"{body.dw_schema}.{dim_table}.{col.name}".lower()
                col_desc = col.description or f"Column {col.name} in {dim_table}"
                
                queries.append(f"""
                    MATCH (t:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(body.dw_schema)}', name: '{escape_for_cypher(dim_table)}'}})
                    MERGE (c:Column {{fqn: '{escape_for_cypher(col_fqn)}'}})
                    SET c.name = '{escape_for_cypher(col.name)}',
                        c.dtype = '{escape_for_cypher(col.dtype)}',
                        c.description = '{escape_for_cypher(col_desc)}',
                        c.is_pk = {str(col.is_pk).lower()},
                        c.is_fk = {str(col.is_fk).lower()},
                        c.updated_at = datetime()
                    MERGE (t)-[:HAS_COLUMN]->(c)
                    RETURN c.name AS name
                """)
                columns_created += 1
            
            # 소스 테이블과의 DERIVED_FROM 관계
            for src in dim.source_tables:
                src_parts = src.split(".")
                src_schema = src_parts[0] if len(src_parts) > 1 else "public"
                src_table = src_parts[-1]
                
                queries.append(f"""
                    MATCH (dw:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(body.dw_schema)}', name: '{escape_for_cypher(dim_table)}'}})
                    MATCH (src:Table {{name: '{escape_for_cypher(src_table)}'}})
                    WHERE toLower(src.schema) = toLower('{escape_for_cypher(src_schema)}')
                    MERGE (dw)-[:DERIVED_FROM {{cube: '{escape_for_cypher(body.cube_name)}'}}]->(src)
                """)
        
        # 3. 팩트 테이블 생성
        fact_table = body.fact_table.name
        fact_desc = f"Fact table for {body.cube_name} cube"
        
        queries.append(f"""
            MERGE (t:Table {{
                db: '{escape_for_cypher(body.db_name)}',
                schema: '{escape_for_cypher(body.dw_schema)}',
                name: '{escape_for_cypher(fact_table)}'
            }})
            SET t.table_type = 'FACT',
                t.cube_name = '{escape_for_cypher(body.cube_name)}',
                t.description = '{escape_for_cypher(fact_desc)}',
                t.updated_at = datetime()
            RETURN t.name AS name
        """)
        tables_created += 1
        
        # Schema -> Table 관계
        queries.append(f"""
            MATCH (s:Schema {{db: '{escape_for_cypher(body.db_name)}', name: '{escape_for_cypher(body.dw_schema)}'}})
            MATCH (t:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(body.dw_schema)}', name: '{escape_for_cypher(fact_table)}'}})
            MERGE (s)-[:HAS_TABLE]->(t)
        """)
        
        # 팩트 테이블 컬럼 생성
        for col in body.fact_table.columns:
            col_fqn = f"{body.dw_schema}.{fact_table}.{col.name}".lower()
            col_desc = col.description or f"Column {col.name} in {fact_table}"
            
            queries.append(f"""
                MATCH (t:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(body.dw_schema)}', name: '{escape_for_cypher(fact_table)}'}})
                MERGE (c:Column {{fqn: '{escape_for_cypher(col_fqn)}'}})
                SET c.name = '{escape_for_cypher(col.name)}',
                    c.dtype = '{escape_for_cypher(col.dtype)}',
                    c.description = '{escape_for_cypher(col_desc)}',
                    c.is_pk = {str(col.is_pk).lower()},
                    c.is_fk = {str(col.is_fk).lower()},
                    c.updated_at = datetime()
                MERGE (t)-[:HAS_COLUMN]->(c)
                RETURN c.name AS name
            """)
            columns_created += 1
            
            # FK 관계 생성
            if col.is_fk and col.fk_target_table:
                fk_parts = col.fk_target_table.split(".")
                fk_schema = fk_parts[0] if len(fk_parts) > 1 else body.dw_schema
                fk_table = fk_parts[-1]
                
                queries.append(f"""
                    MATCH (fact:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(body.dw_schema)}', name: '{escape_for_cypher(fact_table)}'}})
                    MATCH (dim:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(fk_schema)}', name: '{escape_for_cypher(fk_table)}'}})
                    MERGE (fact)-[:FK_TO_TABLE {{column: '{escape_for_cypher(col.name)}'}}]->(dim)
                """)
        
        # 소스 테이블과의 DERIVED_FROM 관계
        for src in body.fact_table.source_tables:
            src_parts = src.split(".")
            src_schema = src_parts[0] if len(src_parts) > 1 else "public"
            src_table = src_parts[-1]
            
            queries.append(f"""
                MATCH (dw:Table {{db: '{escape_for_cypher(body.db_name)}', schema: '{escape_for_cypher(body.dw_schema)}', name: '{escape_for_cypher(fact_table)}'}})
                MATCH (src:Table {{name: '{escape_for_cypher(src_table)}'}})
                WHERE toLower(src.schema) = toLower('{escape_for_cypher(src_schema)}')
                MERGE (dw)-[:DERIVED_FROM {{cube: '{escape_for_cypher(body.cube_name)}'}}]->(src)
            """)
        
        # 쿼리 실행
        logger.info("[API] DW 스타스키마 쿼리 실행 | queries=%d", len(queries))
        await client.execute_queries(queries)
        
        # 4. 벡터 임베딩 생성
        if body.create_embeddings and api_key:
            openai_client = AsyncOpenAI(api_key=api_key)
            embedding_client = EmbeddingClient(openai_client)
            
            # 테이블 임베딩
            table_query = f"""
                MATCH (t:Table)
                WHERE t.schema = '{escape_for_cypher(body.dw_schema)}'
                  AND t.cube_name = '{escape_for_cypher(body.cube_name)}'
                  AND t.description IS NOT NULL
                RETURN elementId(t) AS tid, t.name AS name, t.description AS description
            """
            results = await client.execute_queries([table_query])
            tables = results[0] if results else []
            
            for item in tables:
                text = embedding_client.format_table_text(
                    table_name=item.get("name", ""),
                    description=item.get("description", "")
                )
                vector = await embedding_client.embed_text(text)
                
                if vector:
                    set_query = f"""
                        MATCH (t)
                        WHERE elementId(t) = '{item['tid']}'
                        SET t.vector = {vector}, t.embedding_updated = datetime()
                    """
                    await client.execute_queries([set_query])
                    embeddings_created += 1
            
            # 컬럼 임베딩
            column_query = f"""
                MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                WHERE t.schema = '{escape_for_cypher(body.dw_schema)}'
                  AND t.cube_name = '{escape_for_cypher(body.cube_name)}'
                  AND c.description IS NOT NULL
                RETURN elementId(c) AS cid, c.name AS column_name, t.name AS table_name,
                       coalesce(c.dtype, '') AS dtype, c.description AS description
            """
            results = await client.execute_queries([column_query])
            columns = results[0] if results else []
            
            texts = []
            for item in columns:
                text = embedding_client.format_column_text(
                    column_name=item.get("column_name", ""),
                    table_name=item.get("table_name", ""),
                    dtype=item.get("dtype", ""),
                    description=item.get("description", "")
                )
                texts.append(text)
            
            if texts:
                vectors = await embedding_client.embed_batch(texts)
                for item, vector in zip(columns, vectors):
                    if vector:
                        set_query = f"""
                            MATCH (c)
                            WHERE elementId(c) = '{item['cid']}'
                            SET c.vector = {vector}, c.embedding_updated = datetime()
                        """
                        await client.execute_queries([set_query])
                        embeddings_created += 1
        
        logger.info(
            "[API] DW 스타스키마 등록 완료 | tables=%d | columns=%d | embeddings=%d",
            tables_created, columns_created, embeddings_created
        )
        
        return {
            "success": True,
            "message": f"DW 스타스키마 '{body.cube_name}' 등록 완료",
            "tables_created": tables_created,
            "columns_created": columns_created,
            "embeddings_created": embeddings_created,
            "cube_name": body.cube_name,
            "dw_schema": body.dw_schema
        }
    
    except Exception as e:
        logger.error("[API] DW 스타스키마 등록 실패: %s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()


@router.delete("/schema/dw-tables/{cube_name}")
async def delete_dw_star_schema(
    request: Request,
    cube_name: str,
    dw_schema: str = "dw",
    db_name: str = "postgres"
):
    """DW 스타스키마 테이블/컬럼을 Neo4j에서 삭제
    
    Args:
        cube_name: 삭제할 큐브 이름
        dw_schema: DW 스키마명
        db_name: 데이터베이스 이름
    """
    logger.info("[API] DW 스타스키마 삭제 | cube=%s | schema=%s", cube_name, dw_schema)
    
    client = Neo4jClient()
    
    try:
        # 해당 큐브의 테이블/컬럼/관계 삭제
        delete_queries = [
            # 컬럼 삭제
            f"""
                MATCH (t:Table {{cube_name: '{escape_for_cypher(cube_name)}', schema: '{escape_for_cypher(dw_schema)}'}})-[:HAS_COLUMN]->(c:Column)
                DETACH DELETE c
            """,
            # 테이블 삭제
            f"""
                MATCH (t:Table {{cube_name: '{escape_for_cypher(cube_name)}', schema: '{escape_for_cypher(dw_schema)}'}})
                DETACH DELETE t
            """
        ]
        
        await client.execute_queries(delete_queries)
        
        logger.info("[API] DW 스타스키마 삭제 완료 | cube=%s", cube_name)
        
        return {
            "success": True,
            "message": f"DW 스타스키마 '{cube_name}' 삭제 완료",
            "cube_name": cube_name,
            "dw_schema": dw_schema
        }
    
    except Exception as e:
        logger.error("[API] DW 스타스키마 삭제 실패: %s", e)
        raise HTTPException(500, build_error_body(e))
    finally:
        await client.close()
