"""메타데이터 보강 Phase (Phase 3.5) - DBMS

Text2SQL API를 통해 샘플 데이터를 조회하고,
LLM으로 테이블/컬럼 설명을 생성합니다.
FK 관계도 샘플 데이터 매칭으로 추론합니다.

벡터라이징 전에 실행되어, 벡터 생성 품질을 향상시킵니다.
"""

import logging
from typing import Any, AsyncGenerator

import aiohttp
from openai import AsyncOpenAI

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import AnalysisStats
from config.settings import settings
from service.metadata_enrichment_service import MetadataEnrichmentService
from util.stream_event import emit_message, emit_phase_event
from util.text_utils import log_process


async def run_metadata_phase(
    analyzer: Any,
    client: Neo4jClient,
    orchestrator: Any,
    stats: AnalysisStats,
) -> AsyncGenerator[bytes, None]:
    """Phase 3.5: 메타데이터 보강 (Text2SQL 기반)
    
    1. description이 없는 테이블 목록 조회
    2. Text2SQL Direct API로 샘플 데이터 조회 (LIMIT 25)
    3. LLM으로 테이블/컬럼 설명 생성
    4. Neo4j에 description 업데이트
    5. FK 관계 추론 (선택적)
    
    Args:
        analyzer: DbmsAnalyzer 인스턴스 (공유 상태 접근용)
        client: Neo4j 클라이언트
        orchestrator: 오케스트레이터
        stats: 분석 통계
    """
    log_process("METADATA", "START", "메타데이터 보강 단계 시작", logging.INFO)
    yield emit_message("   🚀 [Phase 3.5] 메타데이터 보강 시작...")
    yield emit_phase_event(
        phase_num=3.5,
        phase_name="메타데이터 보강",
        status="in_progress",
        progress=0,
        details={"step": "init"}
    )
    
    # OpenAI 클라이언트 초기화
    api_key = orchestrator.api_key or settings.llm.api_key
    if not api_key:
        yield emit_message("   ⚠️ OpenAI API 키가 없어 메타데이터 보강을 건너뜁니다")
        return
    
    # Text2SQL API URL 확인
    text2sql_url = settings.metadata_enrichment.text2sql_api_url
    if not text2sql_url:
        log_process("METADATA", "SKIP", "TEXT2SQL_API_URL이 설정되지 않음 - 메타데이터 보강 건너뜀", logging.INFO)
        yield emit_message("   ⏭️ TEXT2SQL_API_URL이 설정되지 않아 메타데이터 보강을 건너뜁니다")
        return
    
    try:
        openai_client = AsyncOpenAI(api_key=api_key)
    except Exception as e:
        log_process("METADATA", "ERROR", f"OpenAI 클라이언트 초기화 실패: {e}", logging.ERROR)
        yield emit_message(f"   ⚠️ OpenAI 클라이언트 초기화 실패: {str(e)[:50]}")
        return
    
    # MetadataEnrichmentService 인스턴스 생성
    enrichment_service = MetadataEnrichmentService(
        client=client,
        openai_client=openai_client,
        text2sql_base_url=text2sql_url,
    )
    
    # =======================================================================
    # 1. description이 없는 테이블 목록 조회
    # =======================================================================
    yield emit_message("   🔍 [Phase 3.5-1] description이 비어있는 테이블 조회 중...")
    
    empty_desc_query = """
    MATCH (t:Table)
    WHERE t.description IS NULL 
       OR t.description = '' 
       OR t.description = 'N/A'
    RETURN t.name AS table_name, t.schema AS schema_name
    ORDER BY t.schema, t.name
    """
    
    tables_to_enrich = []
    try:
        async with analyzer._cypher_lock:
            results = await client.execute_queries([empty_desc_query])
        tables_to_enrich = results[0] if results and len(results) > 0 else []
        
        log_process("METADATA", "QUERY", f"description이 비어있는 테이블 수: {len(tables_to_enrich)}", logging.INFO)
        
        if not tables_to_enrich:
            yield emit_message("   ✅ 모든 테이블에 description이 존재합니다. 보강 불필요.")
            return
        
        total_tables = len(tables_to_enrich)
        yield emit_message(f"   📋 description이 비어있는 테이블: {total_tables}개")
        
    except Exception as e:
        log_process("METADATA", "ERROR", f"테이블 조회 예외: {e}", logging.ERROR, e)
        yield emit_message(f"   ⚠️ 테이블 조회 실패: {str(e)[:100]}")
        return
    
    # =======================================================================
    # 2-4. 각 테이블에 대해 샘플 데이터 조회 및 설명 생성
    # =======================================================================
    yield emit_message("   📊 [Phase 3.5-2] 샘플 데이터 기반 설명 생성 중...")
    
    enriched_count = 0
    tables_updated = 0
    columns_updated = 0
    
    try:
        async with aiohttp.ClientSession() as session:
            # Text2SQL 서버 사용 가능 여부 먼저 확인
            yield emit_message("      🔍 Text2SQL 서버 연결 확인 중...")
            server_available = await enrichment_service.check_text2sql_available(session)
            
            if not server_available:
                log_process("METADATA", "SERVER_UNAVAIL", f"Text2SQL 서버 사용 불가: {text2sql_url}", logging.WARNING)
                yield emit_message(f"   ⚠️ Text2SQL 서버에 연결할 수 없습니다 ({text2sql_url})")
                yield emit_message("   ⏭️ 메타데이터 보강 및 FK 추론을 건너뜁니다")
                return
            
            yield emit_message("      ✅ Text2SQL 서버 연결 확인 완료")
            
            for idx, row in enumerate(tables_to_enrich):
                table_name = row.get("table_name") or row["table_name"]
                schema_name = row.get("schema_name") or row.get("schema", "public")
                full_table_name = f'"{schema_name}"."{table_name}"'
                
                log_process("METADATA", "PROCESS", f"[{idx+1}/{total_tables}] {full_table_name} 처리 중", logging.INFO)
                yield emit_message(f"      🔄 [{idx+1}/{total_tables}] {full_table_name} 분석 중...")
                
                progress = int((idx / total_tables) * 70)  # 0-70% 범위 (FK 추론은 70-100%)
                yield emit_phase_event(
                    phase_num=3.5,
                    phase_name="메타데이터 보강",
                    status="in_progress",
                    progress=progress,
                    details={
                        "current_table": full_table_name,
                        "done": idx,
                        "total": total_tables,
                    },
                )
                
                try:
                    # 2. Text2SQL Direct SQL API로 샘플 데이터 조회
                    sample_sql = f"SELECT * FROM {full_table_name} LIMIT {settings.metadata_enrichment.fk_sample_size}"
                    
                    sample_data = await enrichment_service.fetch_sample_data(session, sample_sql)
                    
                    if not sample_data:
                        yield emit_message(f"         ⚠️ 샘플 데이터 없음, 건너뜀")
                        continue
                    
                    yield emit_message(f"         📦 샘플 데이터 {len(sample_data)}개 조회 완료")
                    
                    # 컬럼 정보도 함께 조회
                    columns_query = """
                    MATCH (t:Table {name: $table_name})-[:HAS_COLUMN]->(c:Column)
                    WHERE t.schema = $schema_name
                    RETURN c.name AS column_name, 
                           coalesce(c.dtype, c.dataType) AS data_type, 
                           c.description AS description
                    ORDER BY c.name
                    """
                    async with analyzer._cypher_lock:
                        col_result = await client.execute_with_params(
                            columns_query,
                            {"table_name": table_name, "schema_name": schema_name},
                        )
                    columns_info = col_result if isinstance(col_result, list) else []
                    
                    # 3. LLM으로 테이블/컬럼 설명 생성
                    yield emit_message(f"         🤖 LLM으로 설명 생성 중...")
                    descriptions = await enrichment_service.generate_descriptions_from_sample(
                        table_name,
                        schema_name,
                        sample_data,
                        columns_info,
                    )
                    
                    if descriptions:
                        # 4. Neo4j에 description 업데이트
                        async with analyzer._cypher_lock:
                            t_updated, c_updated = await enrichment_service.update_descriptions_in_neo4j(
                                table_name, schema_name, descriptions
                            )
                        tables_updated += t_updated
                        columns_updated += c_updated
                        enriched_count += 1
                        yield emit_message(f"         ✓ 설명 생성 완료 (테이블: {t_updated}, 컬럼: {c_updated}개)")
                    
                except Exception as e:
                    log_process("METADATA", "TABLE_ERROR", f"테이블 처리 실패: {full_table_name} - {e}", logging.WARNING)
                    yield emit_message(f"         ⚠️ 처리 실패: {str(e)[:80]}")
                    continue
            
            log_process("METADATA", "ENRICH_DONE", 
                f"메타데이터 보강 완료: {enriched_count}/{total_tables}개 테이블, "
                f"테이블 설명 {tables_updated}개, 컬럼 설명 {columns_updated}개", 
                logging.INFO)
            
            yield emit_message(f"   ✅ 메타데이터 보강 완료: {enriched_count}/{total_tables}개 테이블")
            
            # 통계 업데이트
            if not hasattr(stats, 'tables_enriched'):
                stats.tables_enriched = 0
            if not hasattr(stats, 'columns_enriched'):
                stats.columns_enriched = 0
            stats.tables_enriched = tables_updated
            stats.columns_enriched = columns_updated
            
            # =======================================================================
            # 5. FK 관계 추론 (선택적)
            # =======================================================================
            if settings.metadata_enrichment.fk_inference_enabled:
                async for chunk in _run_fk_inference(
                    analyzer, client, session, enrichment_service, stats
                ):
                    yield chunk
            else:
                yield emit_message("   ⏭️ FK 관계 추론이 비활성화되어 있습니다")
            
            yield emit_phase_event(
                phase_num=3.5,
                phase_name="메타데이터 보강",
                status="completed",
                progress=100,
                details={
                    "tables_enriched": enriched_count,
                    "tables_updated": tables_updated,
                    "columns_updated": columns_updated,
                }
            )
            
    except Exception as e:
        log_process("METADATA", "ERROR", f"메타데이터 보강 중 오류: {e}", logging.ERROR, e)
        yield emit_message(f"   ⚠️ 처리 중 오류 발생: {str(e)[:80]}")


async def _run_fk_inference(
    analyzer: Any,
    client: Neo4jClient,
    session: aiohttp.ClientSession,
    enrichment_service: MetadataEnrichmentService,
    stats: AnalysisStats,
) -> AsyncGenerator[bytes, None]:
    """FK 관계 추론 실행"""
    log_process("FK_INFERENCE", "START", "FK 관계 추론 시작", logging.INFO)
    yield emit_message("   🔗 [Phase 3.5-3] FK 관계 추론 시작...")
    yield emit_phase_event(
        phase_num=3.5,
        phase_name="FK 관계 추론",
        status="in_progress",
        progress=70,
        details={"step": "fk_inference"}
    )
    
    # 1. 모든 테이블과 컬럼 정보 조회
    tables_query = """
    MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
    RETURN t.name AS table_name,
           t.schema AS schema_name,
           collect({
               column_name: c.name,
               data_type: coalesce(c.dtype, c.dataType),
               nullable: c.nullable
           }) AS columns
    ORDER BY t.schema, t.name
    """
    
    try:
        async with analyzer._cypher_lock:
            results = await client.execute_queries([tables_query])
        tables = results[0] if results and len(results) > 0 else []
        
        if len(tables) < 2:
            yield emit_message("   ℹ️ 테이블이 2개 미만이어서 FK 추론을 수행할 수 없습니다")
            return
        
        log_process("FK_INFERENCE", "TABLES", f"분석 대상 테이블 수: {len(tables)}개", logging.INFO)
        yield emit_message(f"   📋 분석 대상 테이블: {len(tables)}개")
        
    except Exception as e:
        log_process("FK_INFERENCE", "ERROR", f"테이블 조회 실패: {e}", logging.ERROR, e)
        yield emit_message(f"   ⚠️ 테이블 조회 실패: {str(e)[:100]}")
        return
    
    # 2. FK 후보 쌍 추출
    yield emit_message("   🔍 컬럼명 유사도 기반 후보 쌍 추출 중...")
    candidates = await enrichment_service.find_fk_candidates(tables)
    
    if not candidates:
        yield emit_message("   ℹ️ FK 후보 쌍이 발견되지 않았습니다")
        return
    
    yield emit_message(f"   📊 후보 쌍 발견: {len(candidates)}개")
    
    # 3. 각 후보에 대해 데이터 매칭 검증
    yield emit_message("   ✅ 샘플 데이터 매칭 검증 중...")
    
    verified_count = 0
    total_candidates = len(candidates)
    
    for idx, candidate in enumerate(candidates):
        if idx % 10 == 0:
            progress = 70 + int((idx / total_candidates) * 30)  # 70-100% 범위
            yield emit_message(f"      🔄 [{idx+1}/{total_candidates}] 검증 중... (확정: {verified_count}개)")
            yield emit_phase_event(
                phase_num=3.5,
                phase_name="FK 관계 추론",
                status="in_progress",
                progress=progress,
                details={"done": idx, "total": total_candidates, "verified": verified_count}
            )
        
        verified = await enrichment_service.verify_fk_relationship(session, candidate)
        
        if verified:
            async with analyzer._cypher_lock:
                await enrichment_service.save_fk_relationship(verified)
            verified_count += 1
            
            yield emit_message(
                f"         ✓ FK 확정: "
                f"{candidate['from_schema']}.{candidate['from_table']}.{candidate['from_column']} → "
                f"{candidate['to_schema']}.{candidate['to_table']}.{candidate['to_column']} "
                f"(유사도: {candidate['similarity']:.0%}, 매칭: {verified['match_ratio']:.0%})"
            )
    
    # 통계 업데이트
    if not hasattr(stats, 'fk_relationships_inferred'):
        stats.fk_relationships_inferred = 0
    stats.fk_relationships_inferred = verified_count
    
    log_process("FK_INFERENCE", "COMPLETE", f"FK 관계 추론 완료: {verified_count}/{total_candidates}개 확정", logging.INFO)
    yield emit_message(f"   ✅ FK 관계 추론 완료: {verified_count}/{total_candidates}개 확정")

