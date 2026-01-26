"""리니지 분석 Phase (Phase 5) - DBMS

dbms_analyzer.py에서 분리된 Phase 5 로직입니다.
모든 로직은 100% 보존되며, 위치만 변경되었습니다.
"""

import logging
import os
from typing import Any, AsyncGenerator, List

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import AnalysisStats
from analyzer.lineage_analyzer import LineageAnalyzer, LineageInfo
from util.stream_event import emit_message, emit_phase_event
from util.text_utils import log_process


async def run_lineage_phase(
    analyzer: Any,
    client: Neo4jClient,
    orchestrator: Any,
    stats: AnalysisStats,
) -> AsyncGenerator[bytes, None]:
    """ETL 패턴 감지 및 데이터 리니지 관계 생성
    
    Stored Procedure가 ETL 역할을 하는지 분석하고,
    Source 테이블 → ETL → Target 테이블 간 데이터 흐름 관계를 생성합니다.
    
    Args:
        analyzer: DbmsAnalyzer 인스턴스 (공유 상태 접근용)
        client: Neo4j 클라이언트
        orchestrator: 오케스트레이터
        stats: 분석 통계
    """
    source_dir = orchestrator.dirs.get("source", "")
    
    if not source_dir or not os.path.exists(source_dir):
        yield emit_message("   ℹ️ SP 파일 없음 → 리니지 분석 건너뜀")
        return
    
    # SP 파일 목록 가져오기
    sql_files: List[str] = []
    for root, _, files in os.walk(source_dir):
        for f in files:
            if f.endswith(".sql"):
                sql_files.append(os.path.join(root, f))
    
    if not sql_files:
        yield emit_message("   ℹ️ SP 파일 없음 → 리니지 분석 건너뜀")
        return
    
    total_files = len(sql_files)
    log_process("LINEAGE", "START", f"리니지 분석 시작: {total_files}개 SP 파일", logging.INFO)
    yield emit_message(f"   🔍 [Phase 5] {total_files}개 SP 파일에서 ETL 패턴 분석 시작...")
    yield emit_phase_event(
        phase_num=5,
        phase_name="리니지 분석",
        status="in_progress",
        progress=0,
        details={"total_files": total_files}
    )
    
    # 리니지 분석기 생성
    lineage_analyzer = LineageAnalyzer(dbms="oracle")
    all_lineages: List[LineageInfo] = []
    files_with_etl = 0
    
    # 각 SP 파일 분석
    for idx, sql_file in enumerate(sql_files, 1):
        file_name = os.path.basename(sql_file)
        progress = int((idx / total_files) * 80)  # 0-80% 범위 (저장은 80-100%)
        
        log_process("LINEAGE", "ANALYZE", f"[{idx}/{total_files}] {file_name} 분석 중", logging.INFO)
        
        try:
            async with aiofiles.open(sql_file, "r", encoding="utf-8", errors="ignore") as f:
                sql_content = await f.read()
            
            # 리니지 분석
            lineages = lineage_analyzer.analyze_sql_content(sql_content, file_name)
            
            # ETL 패턴이 감지된 경우만 저장
            etl_lineages = [l for l in lineages if l.is_etl]
            if etl_lineages:
                files_with_etl += 1
                for l in etl_lineages:
                    l.file_name = file_name
                all_lineages.extend(etl_lineages)
                
                # 상세 정보 로깅: 소스/타겟 테이블 표시
                source_tables = set()
                target_tables = set()
                for l in etl_lineages:
                    source_tables.update(l.source_tables or [])
                    target_tables.update(l.target_tables or [])
                
                log_process("LINEAGE", "ETL_FOUND", 
                    f"{file_name}: ETL {len(etl_lineages)}개 (소스: {len(source_tables)}개, 타겟: {len(target_tables)}개)", 
                    logging.INFO)
                yield emit_message(
                    f"      ✅ [{idx}/{total_files}] {file_name}: ETL {len(etl_lineages)}개 감지"
                )
            else:
                yield emit_message(f"      ⏭️ [{idx}/{total_files}] {file_name}: ETL 패턴 없음")
            
            yield emit_phase_event(
                phase_num=5,
                phase_name="리니지 분석",
                status="in_progress",
                progress=progress,
                details={"current_file": file_name, "done": idx, "total": total_files, "etl_found": len(all_lineages)}
            )
            
        except Exception as e:
            error_msg = f"{file_name} 리니지 분석 실패: {e}"
            log_process("LINEAGE", "ERROR", error_msg, logging.ERROR, e)
            raise RuntimeError(error_msg) from e
    
    log_process("LINEAGE", "SCAN_DONE", f"파일 스캔 완료: {files_with_etl}/{total_files}개 파일에서 ETL 패턴 발견", logging.INFO)
    
    # ETL 패턴이 감지된 경우 Neo4j에 저장
    if all_lineages:
        log_process("LINEAGE", "SAVE_START", f"Neo4j 저장 시작: {len(all_lineages)}개 ETL 패턴", logging.INFO)
        yield emit_message(f"\n   💾 총 {len(all_lineages)}개 ETL 패턴 → Neo4j 저장 중...")
        yield emit_phase_event(
            phase_num=5,
            phase_name="리니지 분석",
            status="in_progress",
            progress=85,
            details={"step": "saving", "etl_count": len(all_lineages)}
        )
        
        try:
            # name_case 옵션 가져오기
            name_case = getattr(orchestrator, "name_case", "original")
            
            result = await lineage_analyzer.save_lineage_to_neo4j(
                client=client,
                lineage_list=all_lineages,
                file_name="",
                name_case=name_case,
            )
            
            # 통계 업데이트
            if not hasattr(stats, 'etl_count'):
                stats.etl_count = 0
            if not hasattr(stats, 'data_flows'):
                stats.data_flows = 0
            
            stats.etl_count = result.get("etl_nodes", 0)
            stats.data_flows = result.get("data_flows", 0)
            
            log_process("LINEAGE", "COMPLETE", 
                f"리니지 저장 완료: ETL {result.get('etl_nodes', 0)}개, "
                f"READS {result.get('etl_reads', 0)}개, WRITES {result.get('etl_writes', 0)}개, "
                f"DATA_FLOWS {result.get('data_flows', 0)}개", 
                logging.INFO)
            
            yield emit_message(
                f"   ✅ 리니지 저장 완료: "
                f"ETL 프로시저 {result.get('etl_nodes', 0)}개, "
                f"ETL_READS {result.get('etl_reads', 0)}개, "
                f"ETL_WRITES {result.get('etl_writes', 0)}개, "
                f"DATA_FLOWS_TO {result.get('data_flows', 0)}개"
            )
            yield emit_phase_event(
                phase_num=5,
                phase_name="리니지 분석",
                status="completed",
                progress=100,
                details={
                    "etl_nodes": result.get('etl_nodes', 0),
                    "etl_reads": result.get('etl_reads', 0),
                    "etl_writes": result.get('etl_writes', 0),
                    "data_flows": result.get('data_flows', 0)
                }
            )
            
        except Exception as e:
            error_msg = f"리니지 저장 실패: {str(e)}"
            yield emit_message(f"   ❌ {error_msg}")
            log_process("LINEAGE", "ERROR", error_msg, logging.ERROR, e)
            raise RuntimeError(error_msg) from e
    else:
        log_process("LINEAGE", "SKIP", "ETL 패턴 없음 - 리니지 관계 미생성", logging.INFO)
        yield emit_message("   ℹ️ ETL 패턴 없음 → 리니지 관계 미생성")
        yield emit_phase_event(
            phase_num=5,
            phase_name="리니지 분석",
            status="completed",
            progress=100,
            details={"etl_nodes": 0, "message": "no_etl_patterns"}
        )

