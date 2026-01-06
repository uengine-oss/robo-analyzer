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

from fastapi import APIRouter, HTTPException, Request
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

def extract_user_id(request: Request) -> str:
    """요청 헤더에서 사용자 ID 추출"""
    user_id = request.headers.get("Session-UUID", "")
    if not user_id:
        raise HTTPException(400, "Session-UUID 헤더가 필요합니다.")
    return user_id


def get_current_timestamp() -> str:
    """현재 시간을 ISO 형식으로 반환"""
    return datetime.utcnow().isoformat() + "Z"


# =============================================================================
# 용어집(Glossary) API
# =============================================================================

@router.get("/")
async def list_glossaries(request: Request):
    """사용자의 모든 용어집 목록 조회"""
    user_id = extract_user_id(request)
    logger.info("[API] 용어집 목록 조회 | user=%s", user_id)
    
    query = f"""
        MATCH (g:Glossary {{user_id: '{escape_for_cypher(user_id)}'}})
        OPTIONAL MATCH (g)-[:HAS_TERM]->(t:Term)
        WITH g, count(t) as termCount
        RETURN 
            elementId(g) as id,
            g.name as name,
            g.description as description,
            g.type as type,
            g.created_at as createdAt,
            g.updated_at as updatedAt,
            termCount
        ORDER BY g.name
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
async def create_glossary(request: Request, body: GlossaryCreate):
    """새 용어집 생성"""
    user_id = extract_user_id(request)
    now = get_current_timestamp()
    logger.info("[API] 용어집 생성 | user=%s | name=%s", user_id, body.name)
    
    query = f"""
        CREATE (g:Glossary {{
            user_id: '{escape_for_cypher(user_id)}',
            name: '{escape_for_cypher(body.name)}',
            description: '{escape_for_cypher(body.description)}',
            type: '{escape_for_cypher(body.type)}',
            created_at: '{now}',
            updated_at: '{now}'
        }})
        RETURN elementId(g) as id, g.name as name
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
async def get_glossary(request: Request, glossary_id: str):
    """특정 용어집 상세 조회"""
    user_id = extract_user_id(request)
    logger.info("[API] 용어집 상세 조회 | user=%s | id=%s", user_id, glossary_id)
    
    query = f"""
        MATCH (g:Glossary)
        WHERE elementId(g) = '{escape_for_cypher(glossary_id)}'
          AND g.user_id = '{escape_for_cypher(user_id)}'
        OPTIONAL MATCH (g)-[:HAS_TERM]->(t:Term)
        WITH g, count(t) as termCount
        RETURN 
            elementId(g) as id,
            g.name as name,
            g.description as description,
            g.type as type,
            g.created_at as createdAt,
            g.updated_at as updatedAt,
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
async def update_glossary(request: Request, glossary_id: str, body: GlossaryUpdate):
    """용어집 정보 수정"""
    user_id = extract_user_id(request)
    now = get_current_timestamp()
    logger.info("[API] 용어집 수정 | user=%s | id=%s", user_id, glossary_id)
    
    # SET 절 동적 생성
    set_clauses = [f"g.updated_at = '{now}'"]
    if body.name is not None:
        set_clauses.append(f"g.name = '{escape_for_cypher(body.name)}'")
    if body.description is not None:
        set_clauses.append(f"g.description = '{escape_for_cypher(body.description)}'")
    if body.type is not None:
        set_clauses.append(f"g.type = '{escape_for_cypher(body.type)}'")
    
    query = f"""
        MATCH (g:Glossary)
        WHERE elementId(g) = '{escape_for_cypher(glossary_id)}'
          AND g.user_id = '{escape_for_cypher(user_id)}'
        SET {', '.join(set_clauses)}
        RETURN elementId(g) as id, g.name as name
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
async def delete_glossary(request: Request, glossary_id: str):
    """용어집 삭제 (포함된 용어도 함께 삭제)"""
    user_id = extract_user_id(request)
    logger.info("[API] 용어집 삭제 | user=%s | id=%s", user_id, glossary_id)
    
    query = f"""
        MATCH (g:Glossary)
        WHERE elementId(g) = '{escape_for_cypher(glossary_id)}'
          AND g.user_id = '{escape_for_cypher(user_id)}'
        OPTIONAL MATCH (g)-[:HAS_TERM]->(t:Term)
        DETACH DELETE g, t
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
    request: Request,
    glossary_id: str,
    status: Optional[str] = None,
    search: Optional[str] = None,
):
    """용어집의 용어 목록 조회"""
    user_id = extract_user_id(request)
    logger.info("[API] 용어 목록 조회 | user=%s | glossary=%s", user_id, glossary_id)
    
    where_clauses = [
        f"elementId(g) = '{escape_for_cypher(glossary_id)}'",
        f"g.user_id = '{escape_for_cypher(user_id)}'"
    ]
    if status:
        where_clauses.append(f"t.status = '{escape_for_cypher(status)}'")
    if search:
        where_clauses.append(
            f"(toLower(t.name) CONTAINS toLower('{escape_for_cypher(search)}') "
            f"OR toLower(t.description) CONTAINS toLower('{escape_for_cypher(search)}'))"
        )
    
    query = f"""
        MATCH (g:Glossary)-[:HAS_TERM]->(t:Term)
        WHERE {' AND '.join(where_clauses)}
        OPTIONAL MATCH (t)-[:OWNED_BY]->(o:Owner)
        OPTIONAL MATCH (t)-[:BELONGS_TO_DOMAIN]->(d:Domain)
        OPTIONAL MATCH (t)-[:HAS_TAG]->(tag:Tag)
        WITH t, 
             collect(DISTINCT {{id: elementId(o), name: o.name}}) as owners,
             collect(DISTINCT {{id: elementId(d), name: d.name}}) as domains,
             collect(DISTINCT {{id: elementId(tag), name: tag.name, color: tag.color}}) as tags
        RETURN 
            elementId(t) as id,
            t.name as name,
            t.description as description,
            t.status as status,
            t.synonyms as synonyms,
            t.created_at as createdAt,
            t.updated_at as updatedAt,
            owners,
            domains,
            tags
        ORDER BY t.name
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
async def create_term(request: Request, glossary_id: str, body: TermCreate):
    """새 용어 생성"""
    user_id = extract_user_id(request)
    now = get_current_timestamp()
    logger.info("[API] 용어 생성 | user=%s | glossary=%s | name=%s", user_id, glossary_id, body.name)
    
    # 용어 생성 쿼리
    synonyms_str = str(body.synonyms).replace("'", "\\'")
    create_query = f"""
        MATCH (g:Glossary)
        WHERE elementId(g) = '{escape_for_cypher(glossary_id)}'
          AND g.user_id = '{escape_for_cypher(user_id)}'
        CREATE (t:Term {{
            user_id: '{escape_for_cypher(user_id)}',
            name: '{escape_for_cypher(body.name)}',
            description: '{escape_for_cypher(body.description)}',
            status: '{escape_for_cypher(body.status)}',
            synonyms: {body.synonyms},
            created_at: '{now}',
            updated_at: '{now}'
        }})
        CREATE (g)-[:HAS_TERM]->(t)
        RETURN elementId(t) as id, t.name as name
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
                    MATCH (t:Term) WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                    MERGE (d:Domain {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(domain_name)}'}})
                    MERGE (t)-[:BELONGS_TO_DOMAIN]->(d)
                """
                await client.execute_queries([domain_query])
        
        # 소유자 연결
        if body.owners:
            for owner_name in body.owners:
                owner_query = f"""
                    MATCH (t:Term) WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                    MERGE (o:Owner {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(owner_name)}'}})
                    MERGE (t)-[:OWNED_BY]->(o)
                """
                await client.execute_queries([owner_query])
        
        # 태그 연결
        if body.tags:
            for tag_name in body.tags:
                tag_query = f"""
                    MATCH (t:Term) WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                    MERGE (tag:Tag {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(tag_name)}'}})
                    ON CREATE SET tag.color = '#3498db'
                    MERGE (t)-[:HAS_TAG]->(tag)
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
async def get_term(request: Request, glossary_id: str, term_id: str):
    """특정 용어 상세 조회"""
    user_id = extract_user_id(request)
    logger.info("[API] 용어 상세 조회 | user=%s | term=%s", user_id, term_id)
    
    query = f"""
        MATCH (g:Glossary)-[:HAS_TERM]->(t:Term)
        WHERE elementId(t) = '{escape_for_cypher(term_id)}'
          AND g.user_id = '{escape_for_cypher(user_id)}'
        OPTIONAL MATCH (t)-[:OWNED_BY]->(o:Owner)
        OPTIONAL MATCH (t)-[:REVIEWED_BY]->(r:Owner)
        OPTIONAL MATCH (t)-[:BELONGS_TO_DOMAIN]->(d:Domain)
        OPTIONAL MATCH (t)-[:HAS_TAG]->(tag:Tag)
        OPTIONAL MATCH (t)-[:RELATED_TO]->(rt:Term)
        WITH t, g,
             collect(DISTINCT {{id: elementId(o), name: o.name, email: o.email}}) as owners,
             collect(DISTINCT {{id: elementId(r), name: r.name, email: r.email}}) as reviewers,
             collect(DISTINCT {{id: elementId(d), name: d.name}}) as domains,
             collect(DISTINCT {{id: elementId(tag), name: tag.name, color: tag.color}}) as tags,
             collect(DISTINCT {{id: elementId(rt), name: rt.name}}) as relatedTerms
        RETURN 
            elementId(t) as id,
            t.name as name,
            t.description as description,
            t.status as status,
            t.synonyms as synonyms,
            t.created_at as createdAt,
            t.updated_at as updatedAt,
            elementId(g) as glossaryId,
            g.name as glossaryName,
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
async def update_term(request: Request, glossary_id: str, term_id: str, body: TermUpdate):
    """용어 정보 수정"""
    user_id = extract_user_id(request)
    now = get_current_timestamp()
    logger.info("[API] 용어 수정 | user=%s | term=%s", user_id, term_id)
    
    # SET 절 동적 생성
    set_clauses = [f"t.updated_at = '{now}'"]
    if body.name is not None:
        set_clauses.append(f"t.name = '{escape_for_cypher(body.name)}'")
    if body.description is not None:
        set_clauses.append(f"t.description = '{escape_for_cypher(body.description)}'")
    if body.status is not None:
        set_clauses.append(f"t.status = '{escape_for_cypher(body.status)}'")
    if body.synonyms is not None:
        set_clauses.append(f"t.synonyms = {body.synonyms}")
    
    update_query = f"""
        MATCH (g:Glossary)-[:HAS_TERM]->(t:Term)
        WHERE elementId(t) = '{escape_for_cypher(term_id)}'
          AND g.user_id = '{escape_for_cypher(user_id)}'
        SET {', '.join(set_clauses)}
        RETURN elementId(t) as id, t.name as name
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
                MATCH (t:Term)-[r:BELONGS_TO_DOMAIN]->()
                WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                DELETE r
            """])
            # 새 관계 생성
            for domain_name in body.domains:
                await client.execute_queries([f"""
                    MATCH (t:Term) WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                    MERGE (d:Domain {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(domain_name)}'}})
                    MERGE (t)-[:BELONGS_TO_DOMAIN]->(d)
                """])
        
        # 소유자 업데이트
        if body.owners is not None:
            await client.execute_queries([f"""
                MATCH (t:Term)-[r:OWNED_BY]->()
                WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                DELETE r
            """])
            for owner_name in body.owners:
                await client.execute_queries([f"""
                    MATCH (t:Term) WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                    MERGE (o:Owner {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(owner_name)}'}})
                    MERGE (t)-[:OWNED_BY]->(o)
                """])
        
        # 검토자 업데이트
        if body.reviewers is not None:
            await client.execute_queries([f"""
                MATCH (t:Term)-[r:REVIEWED_BY]->()
                WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                DELETE r
            """])
            for reviewer_name in body.reviewers:
                await client.execute_queries([f"""
                    MATCH (t:Term) WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                    MERGE (o:Owner {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(reviewer_name)}'}})
                    MERGE (t)-[:REVIEWED_BY]->(o)
                """])
        
        # 태그 업데이트
        if body.tags is not None:
            await client.execute_queries([f"""
                MATCH (t:Term)-[r:HAS_TAG]->()
                WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                DELETE r
            """])
            for tag_name in body.tags:
                await client.execute_queries([f"""
                    MATCH (t:Term) WHERE elementId(t) = '{escape_for_cypher(term_id)}'
                    MERGE (tag:Tag {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(tag_name)}'}})
                    ON CREATE SET tag.color = '#3498db'
                    MERGE (t)-[:HAS_TAG]->(tag)
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
async def delete_term(request: Request, glossary_id: str, term_id: str):
    """용어 삭제"""
    user_id = extract_user_id(request)
    logger.info("[API] 용어 삭제 | user=%s | term=%s", user_id, term_id)
    
    query = f"""
        MATCH (g:Glossary)-[:HAS_TERM]->(t:Term)
        WHERE elementId(t) = '{escape_for_cypher(term_id)}'
          AND g.user_id = '{escape_for_cypher(user_id)}'
        DETACH DELETE t
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
async def list_domains(request: Request):
    """사용자의 모든 도메인 목록 조회"""
    user_id = extract_user_id(request)
    
    query = f"""
        MATCH (d:Domain {{user_id: '{escape_for_cypher(user_id)}'}})
        OPTIONAL MATCH (t:Term)-[:BELONGS_TO_DOMAIN]->(d)
        WITH d, count(t) as termCount
        RETURN elementId(d) as id, d.name as name, d.description as description, termCount
        ORDER BY d.name
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
async def list_owners(request: Request):
    """사용자의 모든 소유자/검토자 목록 조회"""
    user_id = extract_user_id(request)
    
    query = f"""
        MATCH (o:Owner {{user_id: '{escape_for_cypher(user_id)}'}})
        RETURN elementId(o) as id, o.name as name, o.email as email
        ORDER BY o.name
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
async def list_tags(request: Request):
    """사용자의 모든 태그 목록 조회"""
    user_id = extract_user_id(request)
    
    query = f"""
        MATCH (tag:Tag {{user_id: '{escape_for_cypher(user_id)}'}})
        OPTIONAL MATCH (t:Term)-[:HAS_TAG]->(tag)
        WITH tag, count(t) as termCount
        RETURN elementId(tag) as id, tag.name as name, tag.color as color, termCount
        ORDER BY tag.name
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
async def create_domain(request: Request, body: DomainCreate):
    """새 도메인 생성"""
    user_id = extract_user_id(request)
    
    query = f"""
        MERGE (d:Domain {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(body.name)}'}})
        ON CREATE SET d.description = '{escape_for_cypher(body.description)}'
        RETURN elementId(d) as id, d.name as name
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
async def create_owner(request: Request, body: OwnerCreate):
    """새 소유자 생성"""
    user_id = extract_user_id(request)
    
    query = f"""
        MERGE (o:Owner {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(body.name)}'}})
        ON CREATE SET o.email = '{escape_for_cypher(body.email)}', o.role = '{escape_for_cypher(body.role)}'
        RETURN elementId(o) as id, o.name as name
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
async def create_tag(request: Request, body: TagCreate):
    """새 태그 생성"""
    user_id = extract_user_id(request)
    
    query = f"""
        MERGE (tag:Tag {{user_id: '{escape_for_cypher(user_id)}', name: '{escape_for_cypher(body.name)}'}})
        ON CREATE SET tag.color = '{escape_for_cypher(body.color)}'
        RETURN elementId(tag) as id, tag.name as name, tag.color as color
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

