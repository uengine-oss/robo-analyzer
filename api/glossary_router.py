"""용어 관리(Glossary) API 라우터

Neo4j 기반 비즈니스 용어집 관리 기능을 제공합니다.

스키마:
- Glossary: 용어집 (Business_Glossary, Technical_Glossary 등)
- Term: 용어 (Customer, Order 등)
- Domain: 도메인 (Sales, Finance 등)
- Owner: 소유자/검토자
- Tag: 태그

관계:
- (Glossary)-[:HAS_TERM]->(Term)
- (Term)-[:BELONGS_TO_DOMAIN]->(Domain)
- (Term)-[:OWNED_BY]->(Owner)
- (Term)-[:REVIEWED_BY]->(Owner)
- (Term)-[:HAS_TAG]->(Tag)
"""

import logging
from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from analyzer.neo4j_client import Neo4jClient
from config.settings import settings
from util.utility_tool import escape_for_cypher


router = APIRouter(prefix=f"{settings.api_prefix}/glossary")
logger = logging.getLogger(__name__)


# =============================================================================
# 요청/응답 모델
# =============================================================================

class GlossaryCreate(BaseModel):
    """용어집 생성 요청"""
    name: str = Field(..., description="용어집 이름")
    description: str = Field("", description="용어집 설명")
    type: str = Field("Business", description="용어집 유형 (Business, Technical, DataQuality)")


class GlossaryUpdate(BaseModel):
    """용어집 수정 요청"""
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None


class TermCreate(BaseModel):
    """용어 생성 요청"""
    name: str = Field(..., description="용어 이름")
    description: str = Field("", description="용어 설명")
    status: str = Field("Draft", description="상태 (Draft, Pending, Approved, Deprecated)")
    synonyms: List[str] = Field(default_factory=list, description="동의어 목록")
    relatedTerms: List[str] = Field(default_factory=list, description="관련 용어 ID 목록")
    domains: List[str] = Field(default_factory=list, description="도메인 목록")
    owners: List[str] = Field(default_factory=list, description="소유자 목록")
    reviewers: List[str] = Field(default_factory=list, description="검토자 목록")
    tags: List[str] = Field(default_factory=list, description="태그 목록")


class TermUpdate(BaseModel):
    """용어 수정 요청"""
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    synonyms: Optional[List[str]] = None
    relatedTerms: Optional[List[str]] = None
    domains: Optional[List[str]] = None
    owners: Optional[List[str]] = None
    reviewers: Optional[List[str]] = None
    tags: Optional[List[str]] = None


class DomainCreate(BaseModel):
    """도메인 생성 요청"""
    name: str = Field(..., description="도메인 이름")
    description: str = Field("", description="도메인 설명")


class OwnerCreate(BaseModel):
    """소유자 생성 요청"""
    name: str = Field(..., description="소유자 이름")
    email: str = Field("", description="이메일")
    role: str = Field("Owner", description="역할 (Owner, Reviewer)")


class TagCreate(BaseModel):
    """태그 생성 요청"""
    name: str = Field(..., description="태그 이름")
    color: str = Field("#3498db", description="태그 색상")


# =============================================================================
# 유틸리티 함수
# =============================================================================

def get_current_timestamp() -> str:
    """현재 시간을 ISO 형식으로 반환"""
    return datetime.utcnow().isoformat() + "Z"


# =============================================================================
# 용어집(Glossary) API
# =============================================================================

@router.get("/")
async def list_glossaries():
    """모든 용어집 목록 조회"""
    logger.info("[API] 용어집 목록 조회")
    
    query = """
        MATCH (__cy_g__:Glossary)
        OPTIONAL MATCH (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term)
        WITH __cy_g__, count(__cy_t__) as termCount
        RETURN 
            elementId(__cy_g__) as id,
            __cy_g__.name as name,
            __cy_g__.description as description,
            __cy_g__.type as type,
            __cy_g__.created_at as createdAt,
            __cy_g__.updated_at as updatedAt,
            termCount
        ORDER BY __cy_g__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        glossaries = []
        for record in result[0] if result else []:
            glossaries.append({
                "id": record["id"],
                "name": record["name"],
                "description": record.get("description", ""),
                "type": record.get("type", "Business"),
                "termCount": record.get("termCount", 0),
                "createdAt": record.get("createdAt"),
                "updatedAt": record.get("updatedAt"),
            })
        return {"glossaries": glossaries}
    except Exception as e:
        logger.error("[API] 용어집 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 목록 조회 실패: {e}")
    finally:
        await client.close()


@router.post("/")
async def create_glossary(body: GlossaryCreate):
    """새 용어집 생성"""
    now = get_current_timestamp()
    logger.info("[API] 용어집 생성 | name=%s", body.name)
    
    query = f"""
        CREATE (__cy_g__:Glossary {{
            name: '{escape_for_cypher(body.name)}',
            description: '{escape_for_cypher(body.description)}',
            type: '{escape_for_cypher(body.type)}',
            created_at: '{now}',
            updated_at: '{now}'
        }})
        RETURN elementId(__cy_g__) as id, __cy_g__.name as name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {
                "id": result[0][0]["id"],
                "name": result[0][0]["name"],
                "message": "용어집이 생성되었습니다."
            }
        raise HTTPException(500, "용어집 생성 실패")
    except Exception as e:
        logger.error("[API] 용어집 생성 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 생성 실패: {e}")
    finally:
        await client.close()


@router.get("/{glossary_id}")
async def get_glossary(glossary_id: str):
    """특정 용어집 상세 조회"""
    logger.info("[API] 용어집 상세 조회 | id=%s", glossary_id)
    
    query = f"""
        MATCH (__cy_g__:Glossary)
        WHERE elementId(__cy_g__) = '{escape_for_cypher(glossary_id)}'
        OPTIONAL MATCH (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term)
        WITH __cy_g__, count(__cy_t__) as termCount
        RETURN 
            elementId(__cy_g__) as id,
            __cy_g__.name as name,
            __cy_g__.description as description,
            __cy_g__.type as type,
            __cy_g__.created_at as createdAt,
            __cy_g__.updated_at as updatedAt,
            termCount
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if not result or not result[0]:
            raise HTTPException(404, "용어집을 찾을 수 없습니다.")
        
        record = result[0][0]
        return {
            "id": record["id"],
            "name": record["name"],
            "description": record.get("description", ""),
            "type": record.get("type", "Business"),
            "termCount": record.get("termCount", 0),
            "createdAt": record.get("createdAt"),
            "updatedAt": record.get("updatedAt"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어집 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 조회 실패: {e}")
    finally:
        await client.close()


@router.put("/{glossary_id}")
async def update_glossary(glossary_id: str, body: GlossaryUpdate):
    """용어집 정보 수정"""
    now = get_current_timestamp()
    logger.info("[API] 용어집 수정 | id=%s", glossary_id)
    
    # SET 절 동적 생성
    set_clauses = [f"__cy_g__.updated_at = '{now}'"]
    if body.name is not None:
        set_clauses.append(f"__cy_g__.name = '{escape_for_cypher(body.name)}'")
    if body.description is not None:
        set_clauses.append(f"__cy_g__.description = '{escape_for_cypher(body.description)}'")
    if body.type is not None:
        set_clauses.append(f"__cy_g__.type = '{escape_for_cypher(body.type)}'")
    
    query = f"""
        MATCH (__cy_g__:Glossary)
        WHERE elementId(__cy_g__) = '{escape_for_cypher(glossary_id)}'
        SET {', '.join(set_clauses)}
        RETURN elementId(__cy_g__) as id, __cy_g__.name as name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if not result or not result[0]:
            raise HTTPException(404, "용어집을 찾을 수 없습니다.")
        return {"message": "용어집이 수정되었습니다.", "id": result[0][0]["id"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어집 수정 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 수정 실패: {e}")
    finally:
        await client.close()


@router.delete("/{glossary_id}")
async def delete_glossary(glossary_id: str):
    """용어집 삭제 (포함된 용어도 함께 삭제)"""
    logger.info("[API] 용어집 삭제 | id=%s", glossary_id)
    
    query = f"""
        MATCH (__cy_g__:Glossary)
        WHERE elementId(__cy_g__) = '{escape_for_cypher(glossary_id)}'
        OPTIONAL MATCH (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term)
        DETACH DELETE __cy_g__, __cy_t__
    """
    
    client = Neo4jClient()
    try:
        await client.execute_queries([query])
        return {"message": "용어집이 삭제되었습니다."}
    except Exception as e:
        logger.error("[API] 용어집 삭제 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 삭제 실패: {e}")
    finally:
        await client.close()


# =============================================================================
# 용어(Term) API
# =============================================================================

@router.get("/{glossary_id}/terms")
async def list_terms(
    glossary_id: str,
    status: Optional[str] = None,
    search: Optional[str] = None,
):
    """용어집의 용어 목록 조회"""
    logger.info("[API] 용어 목록 조회 | glossary=%s", glossary_id)
    
    where_clauses = [
        f"elementId(__cy_g__) = '{escape_for_cypher(glossary_id)}'"
    ]
    if status:
        where_clauses.append(f"__cy_t__.status = '{escape_for_cypher(status)}'")
    if search:
        where_clauses.append(
            f"(toLower(__cy_t__.name) CONTAINS toLower('{escape_for_cypher(search)}') "
            f"OR toLower(__cy_t__.description) CONTAINS toLower('{escape_for_cypher(search)}'))"
        )
    
    query = f"""
        MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
        WHERE {' AND '.join(where_clauses)}
        OPTIONAL MATCH (__cy_t__)-[:OWNED_BY]->(__cy_o__:Owner)
        OPTIONAL MATCH (__cy_t__)-[:BELONGS_TO_DOMAIN]->(__cy_d__:Domain)
        OPTIONAL MATCH (__cy_t__)-[:HAS_TAG]->(__cy_tag__:Tag)
        WITH __cy_t__, 
             collect(DISTINCT {{id: elementId(__cy_o__), name: __cy_o__.name}}) as owners,
             collect(DISTINCT {{id: elementId(__cy_d__), name: __cy_d__.name}}) as domains,
             collect(DISTINCT {{id: elementId(__cy_tag__), name: __cy_tag__.name, color: __cy_tag__.color}}) as tags
        RETURN 
            elementId(__cy_t__) as id,
            __cy_t__.name as name,
            __cy_t__.description as description,
            __cy_t__.status as status,
            __cy_t__.synonyms as synonyms,
            __cy_t__.created_at as createdAt,
            __cy_t__.updated_at as updatedAt,
            owners,
            domains,
            tags
        ORDER BY __cy_t__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        terms = []
        for record in result[0] if result else []:
            # null 값 필터링
            owners = [o for o in record.get("owners", []) if o.get("id")]
            domains = [d for d in record.get("domains", []) if d.get("id")]
            tags = [t for t in record.get("tags", []) if t.get("id")]
            
            terms.append({
                "id": record["id"],
                "name": record["name"],
                "description": record.get("description", ""),
                "status": record.get("status", "Draft"),
                "synonyms": record.get("synonyms") or [],
                "owners": owners,
                "domains": domains,
                "tags": tags,
                "createdAt": record.get("createdAt"),
                "updatedAt": record.get("updatedAt"),
            })
        return {"terms": terms}
    except Exception as e:
        logger.error("[API] 용어 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어 목록 조회 실패: {e}")
    finally:
        await client.close()


@router.post("/{glossary_id}/terms")
async def create_term(glossary_id: str, body: TermCreate):
    """새 용어 생성"""
    now = get_current_timestamp()
    logger.info("[API] 용어 생성 | glossary=%s | name=%s", glossary_id, body.name)
    
    # 용어 생성 쿼리
    create_query = f"""
        MATCH (__cy_g__:Glossary)
        WHERE elementId(__cy_g__) = '{escape_for_cypher(glossary_id)}'
        CREATE (__cy_t__:Term {{
            name: '{escape_for_cypher(body.name)}',
            description: '{escape_for_cypher(body.description)}',
            status: '{escape_for_cypher(body.status)}',
            synonyms: {body.synonyms},
            created_at: '{now}',
            updated_at: '{now}'
        }})
        CREATE (__cy_g__)-[:HAS_TERM]->(__cy_t__)
        RETURN elementId(__cy_t__) as id, __cy_t__.name as name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([create_query])
        if not result or not result[0]:
            raise HTTPException(404, "용어집을 찾을 수 없습니다.")
        
        term_id = result[0][0]["id"]
        
        # 도메인 연결
        if body.domains:
            for domain_name in body.domains:
                domain_query = f"""
                    MATCH (__cy_t__:Term) WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                    MERGE (__cy_d__:Domain {{name: '{escape_for_cypher(domain_name)}'}})
                    MERGE (__cy_t__)-[:BELONGS_TO_DOMAIN]->(__cy_d__)
                """
                await client.execute_queries([domain_query])
        
        # 소유자 연결
        if body.owners:
            for owner_name in body.owners:
                owner_query = f"""
                    MATCH (__cy_t__:Term) WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                    MERGE (__cy_o__:Owner {{name: '{escape_for_cypher(owner_name)}'}})
                    MERGE (__cy_t__)-[:OWNED_BY]->(__cy_o__)
                """
                await client.execute_queries([owner_query])
        
        # 태그 연결
        if body.tags:
            for tag_name in body.tags:
                tag_query = f"""
                    MATCH (__cy_t__:Term) WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                    MERGE (__cy_tag__:Tag {{name: '{escape_for_cypher(tag_name)}'}})
                    ON CREATE SET __cy_tag__.color = '#3498db'
                    MERGE (__cy_t__)-[:HAS_TAG]->(__cy_tag__)
                """
                await client.execute_queries([tag_query])
        
        return {
            "id": term_id,
            "name": body.name,
            "message": "용어가 생성되었습니다."
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 생성 실패 | error=%s", e)
        raise HTTPException(500, f"용어 생성 실패: {e}")
    finally:
        await client.close()


@router.get("/{glossary_id}/terms/{term_id}")
async def get_term(glossary_id: str, term_id: str):
    """특정 용어 상세 조회"""
    logger.info("[API] 용어 상세 조회 | term=%s", term_id)
    
    query = f"""
        MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
        WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
        OPTIONAL MATCH (__cy_t__)-[:OWNED_BY]->(__cy_o__:Owner)
        OPTIONAL MATCH (__cy_t__)-[:REVIEWED_BY]->(__cy_r__:Owner)
        OPTIONAL MATCH (__cy_t__)-[:BELONGS_TO_DOMAIN]->(__cy_d__:Domain)
        OPTIONAL MATCH (__cy_t__)-[:HAS_TAG]->(__cy_tag__:Tag)
        OPTIONAL MATCH (__cy_t__)-[:RELATED_TO]->(__cy_rt__:Term)
        WITH __cy_t__, __cy_g__,
             collect(DISTINCT {{id: elementId(__cy_o__), name: __cy_o__.name, email: __cy_o__.email}}) as owners,
             collect(DISTINCT {{id: elementId(__cy_r__), name: __cy_r__.name, email: __cy_r__.email}}) as reviewers,
             collect(DISTINCT {{id: elementId(__cy_d__), name: __cy_d__.name}}) as domains,
             collect(DISTINCT {{id: elementId(__cy_tag__), name: __cy_tag__.name, color: __cy_tag__.color}}) as tags,
             collect(DISTINCT {{id: elementId(__cy_rt__), name: __cy_rt__.name}}) as relatedTerms
        RETURN 
            elementId(__cy_t__) as id,
            __cy_t__.name as name,
            __cy_t__.description as description,
            __cy_t__.status as status,
            __cy_t__.synonyms as synonyms,
            __cy_t__.created_at as createdAt,
            __cy_t__.updated_at as updatedAt,
            elementId(__cy_g__) as glossaryId,
            __cy_g__.name as glossaryName,
            owners,
            reviewers,
            domains,
            tags,
            relatedTerms
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if not result or not result[0]:
            raise HTTPException(404, "용어를 찾을 수 없습니다.")
        
        record = result[0][0]
        # null 값 필터링
        owners = [o for o in record.get("owners", []) if o.get("id")]
        reviewers = [r for r in record.get("reviewers", []) if r.get("id")]
        domains = [d for d in record.get("domains", []) if d.get("id")]
        tags = [t for t in record.get("tags", []) if t.get("id")]
        related = [r for r in record.get("relatedTerms", []) if r.get("id")]
        
        return {
            "id": record["id"],
            "name": record["name"],
            "description": record.get("description", ""),
            "status": record.get("status", "Draft"),
            "synonyms": record.get("synonyms") or [],
            "glossaryId": record["glossaryId"],
            "glossaryName": record["glossaryName"],
            "owners": owners,
            "reviewers": reviewers,
            "domains": domains,
            "tags": tags,
            "relatedTerms": related,
            "createdAt": record.get("createdAt"),
            "updatedAt": record.get("updatedAt"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어 조회 실패: {e}")
    finally:
        await client.close()


@router.put("/{glossary_id}/terms/{term_id}")
async def update_term(glossary_id: str, term_id: str, body: TermUpdate):
    """용어 정보 수정"""
    now = get_current_timestamp()
    logger.info("[API] 용어 수정 | term=%s", term_id)
    
    # SET 절 동적 생성
    set_clauses = [f"__cy_t__.updated_at = '{now}'"]
    if body.name is not None:
        set_clauses.append(f"__cy_t__.name = '{escape_for_cypher(body.name)}'")
    if body.description is not None:
        set_clauses.append(f"__cy_t__.description = '{escape_for_cypher(body.description)}'")
    if body.status is not None:
        set_clauses.append(f"__cy_t__.status = '{escape_for_cypher(body.status)}'")
    if body.synonyms is not None:
        set_clauses.append(f"__cy_t__.synonyms = {body.synonyms}")
    
    update_query = f"""
        MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
        WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
        SET {', '.join(set_clauses)}
        RETURN elementId(__cy_t__) as id, __cy_t__.name as name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([update_query])
        if not result or not result[0]:
            raise HTTPException(404, "용어를 찾을 수 없습니다.")
        
        # 도메인 업데이트
        if body.domains is not None:
            # 기존 관계 삭제
            await client.execute_queries([f"""
                MATCH (__cy_t__:Term)-[__cy_r__:BELONGS_TO_DOMAIN]->()
                WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                DELETE __cy_r__
            """])
            # 새 관계 생성
            for domain_name in body.domains:
                await client.execute_queries([f"""
                    MATCH (__cy_t__:Term) WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                    MERGE (__cy_d__:Domain {{name: '{escape_for_cypher(domain_name)}'}})
                    MERGE (__cy_t__)-[:BELONGS_TO_DOMAIN]->(__cy_d__)
                """])
        
        # 소유자 업데이트
        if body.owners is not None:
            await client.execute_queries([f"""
                MATCH (__cy_t__:Term)-[__cy_r__:OWNED_BY]->()
                WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                DELETE __cy_r__
            """])
            for owner_name in body.owners:
                await client.execute_queries([f"""
                    MATCH (__cy_t__:Term) WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                    MERGE (__cy_o__:Owner {{name: '{escape_for_cypher(owner_name)}'}})
                    MERGE (__cy_t__)-[:OWNED_BY]->(__cy_o__)
                """])
        
        # 검토자 업데이트
        if body.reviewers is not None:
            await client.execute_queries([f"""
                MATCH (__cy_t__:Term)-[__cy_r__:REVIEWED_BY]->()
                WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                DELETE __cy_r__
            """])
            for reviewer_name in body.reviewers:
                await client.execute_queries([f"""
                    MATCH (__cy_t__:Term) WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                    MERGE (__cy_o__:Owner {{name: '{escape_for_cypher(reviewer_name)}'}})
                    MERGE (__cy_t__)-[:REVIEWED_BY]->(__cy_o__)
                """])
        
        # 태그 업데이트
        if body.tags is not None:
            await client.execute_queries([f"""
                MATCH (__cy_t__:Term)-[__cy_r__:HAS_TAG]->()
                WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                DELETE __cy_r__
            """])
            for tag_name in body.tags:
                await client.execute_queries([f"""
                    MATCH (__cy_t__:Term) WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
                    MERGE (__cy_tag__:Tag {{name: '{escape_for_cypher(tag_name)}'}})
                    ON CREATE SET __cy_tag__.color = '#3498db'
                    MERGE (__cy_t__)-[:HAS_TAG]->(__cy_tag__)
                """])
        
        return {"message": "용어가 수정되었습니다.", "id": result[0][0]["id"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 수정 실패 | error=%s", e)
        raise HTTPException(500, f"용어 수정 실패: {e}")
    finally:
        await client.close()


@router.delete("/{glossary_id}/terms/{term_id}")
async def delete_term(glossary_id: str, term_id: str):
    """용어 삭제"""
    logger.info("[API] 용어 삭제 | term=%s", term_id)
    
    query = f"""
        MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
        WHERE elementId(__cy_t__) = '{escape_for_cypher(term_id)}'
        DETACH DELETE __cy_t__
    """
    
    client = Neo4jClient()
    try:
        await client.execute_queries([query])
        return {"message": "용어가 삭제되었습니다."}
    except Exception as e:
        logger.error("[API] 용어 삭제 실패 | error=%s", e)
        raise HTTPException(500, f"용어 삭제 실패: {e}")
    finally:
        await client.close()


# =============================================================================
# 도메인/소유자/태그 API
# =============================================================================

@router.get("/meta/domains")
async def list_domains():
    """모든 도메인 목록 조회"""
    
    query = """
        MATCH (__cy_d__:Domain)
        OPTIONAL MATCH (__cy_t__:Term)-[:BELONGS_TO_DOMAIN]->(__cy_d__)
        WITH __cy_d__, count(__cy_t__) as termCount
        RETURN elementId(__cy_d__) as id, __cy_d__.name as name, __cy_d__.description as description, termCount
        ORDER BY __cy_d__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        domains = [
            {"id": r["id"], "name": r["name"], "description": r.get("description", ""), "termCount": r["termCount"]}
            for r in (result[0] if result else [])
        ]
        return {"domains": domains}
    except Exception as e:
        raise HTTPException(500, f"도메인 목록 조회 실패: {e}")
    finally:
        await client.close()


@router.get("/meta/owners")
async def list_owners():
    """모든 소유자/검토자 목록 조회"""
    
    query = """
        MATCH (__cy_o__:Owner)
        RETURN elementId(__cy_o__) as id, __cy_o__.name as name, __cy_o__.email as email
        ORDER BY __cy_o__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        owners = [
            {"id": r["id"], "name": r["name"], "email": r.get("email", "")}
            for r in (result[0] if result else [])
        ]
        return {"owners": owners}
    except Exception as e:
        raise HTTPException(500, f"소유자 목록 조회 실패: {e}")
    finally:
        await client.close()


@router.get("/meta/tags")
async def list_tags():
    """모든 태그 목록 조회"""
    
    query = """
        MATCH (__cy_tag__:Tag)
        OPTIONAL MATCH (__cy_t__:Term)-[:HAS_TAG]->(__cy_tag__)
        WITH __cy_tag__, count(__cy_t__) as termCount
        RETURN elementId(__cy_tag__) as id, __cy_tag__.name as name, __cy_tag__.color as color, termCount
        ORDER BY __cy_tag__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        tags = [
            {"id": r["id"], "name": r["name"], "color": r.get("color", "#3498db"), "termCount": r["termCount"]}
            for r in (result[0] if result else [])
        ]
        return {"tags": tags}
    except Exception as e:
        raise HTTPException(500, f"태그 목록 조회 실패: {e}")
    finally:
        await client.close()


@router.post("/meta/domains")
async def create_domain(body: DomainCreate):
    """새 도메인 생성"""
    
    query = f"""
        MERGE (__cy_d__:Domain {{name: '{escape_for_cypher(body.name)}'}})
        ON CREATE SET __cy_d__.description = '{escape_for_cypher(body.description)}'
        RETURN elementId(__cy_d__) as id, __cy_d__.name as name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {"id": result[0][0]["id"], "name": result[0][0]["name"]}
        raise HTTPException(500, "도메인 생성 실패")
    except Exception as e:
        raise HTTPException(500, f"도메인 생성 실패: {e}")
    finally:
        await client.close()


@router.post("/meta/owners")
async def create_owner(body: OwnerCreate):
    """새 소유자 생성"""
    
    query = f"""
        MERGE (__cy_o__:Owner {{name: '{escape_for_cypher(body.name)}'}})
        ON CREATE SET __cy_o__.email = '{escape_for_cypher(body.email)}', __cy_o__.role = '{escape_for_cypher(body.role)}'
        RETURN elementId(__cy_o__) as id, __cy_o__.name as name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {"id": result[0][0]["id"], "name": result[0][0]["name"]}
        raise HTTPException(500, "소유자 생성 실패")
    except Exception as e:
        raise HTTPException(500, f"소유자 생성 실패: {e}")
    finally:
        await client.close()


@router.post("/meta/tags")
async def create_tag(body: TagCreate):
    """새 태그 생성"""
    
    query = f"""
        MERGE (__cy_tag__:Tag {{name: '{escape_for_cypher(body.name)}'}})
        ON CREATE SET __cy_tag__.color = '{escape_for_cypher(body.color)}'
        RETURN elementId(__cy_tag__) as id, __cy_tag__.name as name, __cy_tag__.color as color
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {"id": result[0][0]["id"], "name": result[0][0]["name"], "color": result[0][0]["color"]}
        raise HTTPException(500, "태그 생성 실패")
    except Exception as e:
        raise HTTPException(500, f"태그 생성 실패: {e}")
    finally:
        await client.close()
