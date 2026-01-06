"""ROBO Analyzer API 라우터

엔드포인트:
- POST /robo/analyze/      : 소스 파일 분석 → Neo4j 그래프 생성
- POST /robo/detect-types/ : 파일 내용 기반 타입 자동 감지
- GET /robo/check-data/    : Neo4j에 기존 데이터 존재 여부 확인
- DELETE /robo/data/       : 사용자 데이터 전체 삭제
- GET /robo/lineage/       : 데이터 리니지 그래프 조회
- POST /robo/lineage/analyze/ : ETL 코드에서 리니지 추출
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
    if not file_names:
        raise HTTPException(400, "분석할 소스 파일이 없습니다.")

    logger.info(
        "[API] 분석 시작 | project=%s | strategy=%s | files=%d",
        orchestrator.project_name,
        orchestrator.strategy,
        len(file_names),
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


@router.delete("/delete/")
async def delete_user_data(request: Request):
    """사용자 데이터 전체 삭제 (임시 파일 + Neo4j 그래프)
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
    """
    user_id = extract_user_id(request)
    logger.info("[API] 데이터 삭제 요청 | user=%s", user_id)

    try:
        await AnalysisOrchestrator(
            user_id=user_id,
            api_key="",
            locale="",
            project_name="",
        ).cleanup_all_data()
        logger.info("[API] 데이터 삭제 완료 | user=%s", user_id)
    except Exception as e:
        logger.error("[API] 데이터 삭제 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))

    return {"message": "모든 데이터가 삭제되었습니다."}


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
