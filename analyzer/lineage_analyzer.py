"""데이터 리니지 분석기

ETL 코드에서 데이터 흐름(Source → Target)을 추출하여 Neo4j에 저장합니다.

주요 기능:
- INSERT/MERGE 문에서 타겟 테이블 추출
- SELECT/FROM/JOIN 절에서 소스 테이블 추출
- 데이터 흐름 관계(DATA_FLOW) 생성
"""

import re
import logging
from typing import Optional
from dataclasses import dataclass, field

from analyzer.neo4j_client import Neo4jClient
from util.utility_tool import escape_for_cypher, log_process


@dataclass
class LineageInfo:
    """데이터 리니지 정보"""
    etl_name: str  # ETL 프로시저/함수명
    source_tables: list[str] = field(default_factory=list)
    target_tables: list[str] = field(default_factory=list)
    operation_type: str = "ETL"  # ETL, INSERT, MERGE, UPDATE, DELETE
    description: str = ""


class LineageAnalyzer:
    """ETL 코드에서 데이터 리니지를 분석하는 클래스"""
    
    # SQL 키워드 패턴
    _PROC_PATTERN = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(\w+)",
        re.IGNORECASE
    )
    
    _INSERT_PATTERN = re.compile(
        r"INSERT\s+INTO\s+(\w+(?:\.\w+)?)",
        re.IGNORECASE
    )
    
    _MERGE_PATTERN = re.compile(
        r"MERGE\s+INTO\s+(\w+(?:\.\w+)?)",
        re.IGNORECASE
    )
    
    _UPDATE_PATTERN = re.compile(
        r"UPDATE\s+(\w+(?:\.\w+)?)\s+SET",
        re.IGNORECASE
    )
    
    _DELETE_PATTERN = re.compile(
        r"DELETE\s+FROM\s+(\w+(?:\.\w+)?)",
        re.IGNORECASE
    )
    
    _FROM_PATTERN = re.compile(
        r"FROM\s+(\w+(?:\.\w+)?)",
        re.IGNORECASE
    )
    
    _JOIN_PATTERN = re.compile(
        r"(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+)?JOIN\s+(\w+(?:\.\w+)?)",
        re.IGNORECASE
    )
    
    _USING_PATTERN = re.compile(
        r"USING\s*\(\s*SELECT.*?FROM\s+(\w+(?:\.\w+)?)",
        re.IGNORECASE | re.DOTALL
    )
    
    # 제외할 시스템 테이블/함수
    _EXCLUDED_TABLES = {
        "dual", "sysdate", "systimestamp", "user", "rownum",
        "all_tables", "user_tables", "dba_tables",
        "information_schema", "pg_catalog",
    }
    
    def __init__(
        self,
        user_id: str,
        project_name: str,
        dbms: str = "oracle",
    ):
        self.user_id = user_id
        self.project_name = project_name
        self.dbms = dbms.lower()
    
    def analyze_sql_content(self, sql_content: str, file_name: str = "") -> list[LineageInfo]:
        """SQL 내용을 분석하여 리니지 정보를 추출합니다.
        
        Args:
            sql_content: SQL 소스 코드 문자열
            file_name: 파일명 (로깅용)
            
        Returns:
            LineageInfo 리스트
        """
        lineage_list: list[LineageInfo] = []
        
        # 프로시저/함수 단위로 분석
        procedures = self._split_procedures(sql_content)
        
        for proc_name, proc_body in procedures:
            lineage = self._analyze_procedure(proc_name, proc_body)
            if lineage.source_tables or lineage.target_tables:
                lineage_list.append(lineage)
                log_process(
                    "LINEAGE", "ANALYZE",
                    f"{proc_name}: {len(lineage.source_tables)} sources → {len(lineage.target_tables)} targets"
                )
        
        # 프로시저가 없으면 파일 전체를 하나의 단위로 분석
        if not procedures:
            lineage = self._analyze_procedure(file_name or "UNKNOWN", sql_content)
            if lineage.source_tables or lineage.target_tables:
                lineage_list.append(lineage)
        
        return lineage_list
    
    def _split_procedures(self, sql_content: str) -> list[tuple[str, str]]:
        """SQL 내용을 프로시저/함수 단위로 분할합니다."""
        result = []
        
        # CREATE PROCEDURE/FUNCTION 찾기
        pattern = re.compile(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(\w+)\s*"
            r"(?:\([^)]*\))?\s*(?:AS|IS)?\s*"
            r"(.*?)(?=CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)|$)",
            re.IGNORECASE | re.DOTALL
        )
        
        for match in pattern.finditer(sql_content):
            proc_name = match.group(1)
            proc_body = match.group(2)
            result.append((proc_name, proc_body))
        
        return result
    
    def _analyze_procedure(self, proc_name: str, proc_body: str) -> LineageInfo:
        """단일 프로시저의 리니지를 분석합니다."""
        lineage = LineageInfo(etl_name=proc_name)
        
        # 타겟 테이블 추출 (INSERT, MERGE, UPDATE, DELETE)
        targets = set()
        
        for match in self._INSERT_PATTERN.finditer(proc_body):
            table = self._normalize_table_name(match.group(1))
            if table and table.lower() not in self._EXCLUDED_TABLES:
                targets.add(table)
                lineage.operation_type = "INSERT"
        
        for match in self._MERGE_PATTERN.finditer(proc_body):
            table = self._normalize_table_name(match.group(1))
            if table and table.lower() not in self._EXCLUDED_TABLES:
                targets.add(table)
                lineage.operation_type = "MERGE"
        
        for match in self._UPDATE_PATTERN.finditer(proc_body):
            table = self._normalize_table_name(match.group(1))
            if table and table.lower() not in self._EXCLUDED_TABLES:
                targets.add(table)
                if not lineage.operation_type or lineage.operation_type == "ETL":
                    lineage.operation_type = "UPDATE"
        
        for match in self._DELETE_PATTERN.finditer(proc_body):
            table = self._normalize_table_name(match.group(1))
            if table and table.lower() not in self._EXCLUDED_TABLES:
                targets.add(table)
                if not lineage.operation_type or lineage.operation_type == "ETL":
                    lineage.operation_type = "DELETE"
        
        # 소스 테이블 추출 (FROM, JOIN, USING)
        sources = set()
        
        for match in self._FROM_PATTERN.finditer(proc_body):
            table = self._normalize_table_name(match.group(1))
            if table and table.lower() not in self._EXCLUDED_TABLES:
                # 타겟 테이블은 소스에서 제외 (자기 자신 참조 제외)
                if table.upper() not in {t.upper() for t in targets}:
                    sources.add(table)
        
        for match in self._JOIN_PATTERN.finditer(proc_body):
            table = self._normalize_table_name(match.group(1))
            if table and table.lower() not in self._EXCLUDED_TABLES:
                if table.upper() not in {t.upper() for t in targets}:
                    sources.add(table)
        
        for match in self._USING_PATTERN.finditer(proc_body):
            table = self._normalize_table_name(match.group(1))
            if table and table.lower() not in self._EXCLUDED_TABLES:
                if table.upper() not in {t.upper() for t in targets}:
                    sources.add(table)
        
        lineage.source_tables = sorted(sources)
        lineage.target_tables = sorted(targets)
        
        # ETL이 여러 작업을 포함하면 ETL로 표시
        if len(targets) > 1 or (lineage.source_tables and lineage.target_tables):
            lineage.operation_type = "ETL"
        
        return lineage
    
    def _normalize_table_name(self, table: str) -> Optional[str]:
        """테이블명을 정규화합니다."""
        if not table:
            return None
        
        # 스키마.테이블 형식 유지
        parts = table.strip().split(".")
        normalized = ".".join(p.strip().upper() for p in parts if p.strip())
        
        return normalized if normalized else None
    
    async def save_lineage_to_neo4j(
        self,
        client: Neo4jClient,
        lineage_list: list[LineageInfo],
        file_name: str = "",
    ) -> dict:
        """리니지 정보를 Neo4j에 저장합니다.
        
        Args:
            client: Neo4j 클라이언트
            lineage_list: LineageInfo 리스트
            file_name: 원본 파일명
            
        Returns:
            저장 결과 (노드/관계 수)
        """
        queries = []
        stats = {"etl_nodes": 0, "data_sources": 0, "data_flows": 0}
        
        user_id = escape_for_cypher(self.user_id)
        project_name = escape_for_cypher(self.project_name)
        
        for lineage in lineage_list:
            etl_name = escape_for_cypher(lineage.etl_name)
            
            # ETL 프로세스 노드 생성
            queries.append(f"""
                MERGE (etl:ETLProcess {{
                    user_id: '{user_id}',
                    project_name: '{project_name}',
                    name: '{etl_name}'
                }})
                SET etl.operation_type = '{lineage.operation_type}',
                    etl.file_name = '{escape_for_cypher(file_name)}',
                    etl.source_count = {len(lineage.source_tables)},
                    etl.target_count = {len(lineage.target_tables)}
                RETURN etl
            """)
            stats["etl_nodes"] += 1
            
            # 소스 테이블 → DataSource 노드 + DATA_FLOW_FROM 관계
            for source in lineage.source_tables:
                source_name = escape_for_cypher(source)
                queries.append(f"""
                    MERGE (ds:DataSource {{
                        user_id: '{user_id}',
                        project_name: '{project_name}',
                        name: '{source_name}'
                    }})
                    SET ds.type = 'SOURCE'
                    RETURN ds
                """)
                stats["data_sources"] += 1
                
                # 소스 → ETL 관계
                queries.append(f"""
                    MATCH (ds:DataSource {{
                        user_id: '{user_id}',
                        project_name: '{project_name}',
                        name: '{source_name}'
                    }})
                    MATCH (etl:ETLProcess {{
                        user_id: '{user_id}',
                        project_name: '{project_name}',
                        name: '{etl_name}'
                    }})
                    MERGE (ds)-[r:DATA_FLOW_TO]->(etl)
                    SET r.flow_type = 'SOURCE_TO_ETL'
                    RETURN ds, r, etl
                """)
                stats["data_flows"] += 1
            
            # 타겟 테이블 → DataSource 노드 + DATA_FLOW_TO 관계
            for target in lineage.target_tables:
                target_name = escape_for_cypher(target)
                queries.append(f"""
                    MERGE (ds:DataSource {{
                        user_id: '{user_id}',
                        project_name: '{project_name}',
                        name: '{target_name}'
                    }})
                    SET ds.type = 'TARGET'
                    RETURN ds
                """)
                stats["data_sources"] += 1
                
                # ETL → 타겟 관계
                queries.append(f"""
                    MATCH (etl:ETLProcess {{
                        user_id: '{user_id}',
                        project_name: '{project_name}',
                        name: '{etl_name}'
                    }})
                    MATCH (ds:DataSource {{
                        user_id: '{user_id}',
                        project_name: '{project_name}',
                        name: '{target_name}'
                    }})
                    MERGE (etl)-[r:DATA_FLOW_TO]->(ds)
                    SET r.flow_type = 'ETL_TO_TARGET'
                    RETURN etl, r, ds
                """)
                stats["data_flows"] += 1
            
            # 소스 → 타겟 직접 연결 (전체 흐름 표시용)
            for source in lineage.source_tables:
                for target in lineage.target_tables:
                    source_name = escape_for_cypher(source)
                    target_name = escape_for_cypher(target)
                    queries.append(f"""
                        MATCH (src:DataSource {{
                            user_id: '{user_id}',
                            project_name: '{project_name}',
                            name: '{source_name}'
                        }})
                        MATCH (tgt:DataSource {{
                            user_id: '{user_id}',
                            project_name: '{project_name}',
                            name: '{target_name}'
                        }})
                        MERGE (src)-[r:TRANSFORMS_TO]->(tgt)
                        SET r.via_etl = '{etl_name}',
                            r.operation = '{lineage.operation_type}'
                        RETURN src, r, tgt
                    """)
        
        if queries:
            await client.execute_queries(queries)
            log_process(
                "LINEAGE", "SAVE",
                f"Neo4j 저장 완료: ETL {stats['etl_nodes']}개, "
                f"DataSource {stats['data_sources']}개, Flow {stats['data_flows']}개"
            )
        
        return stats


async def analyze_lineage_from_sql(
    sql_content: str,
    user_id: str,
    project_name: str,
    file_name: str = "",
    dbms: str = "oracle",
) -> tuple[list[LineageInfo], dict]:
    """SQL 내용에서 리니지를 분석하고 Neo4j에 저장합니다.
    
    Args:
        sql_content: SQL 소스 코드
        user_id: 사용자 ID
        project_name: 프로젝트명
        file_name: 파일명
        dbms: DBMS 타입
        
    Returns:
        (LineageInfo 리스트, 저장 통계)
    """
    analyzer = LineageAnalyzer(
        user_id=user_id,
        project_name=project_name,
        dbms=dbms,
    )
    
    lineage_list = analyzer.analyze_sql_content(sql_content, file_name)
    
    if lineage_list:
        client = Neo4jClient()
        try:
            stats = await analyzer.save_lineage_to_neo4j(client, lineage_list, file_name)
        finally:
            await client.close()
    else:
        stats = {"etl_nodes": 0, "data_sources": 0, "data_flows": 0}
    
    return lineage_list, stats

