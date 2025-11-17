#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stored Procedure 분석 및 레포트 생성 프로그램
Neo4j에서 추출한 Stored Procedure 데이터를 분석하여 상세 레포트를 생성합니다.
"""

import json
import re
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional
from collections import defaultdict


def html_escape(text: str) -> str:
    """HTML 특수 문자 이스케이프"""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#x27;"))


@dataclass
class TableInfo:
    """테이블 정보"""
    name: str
    operations: Set[str] = field(default_factory=set)  # SELECT, INSERT, UPDATE, DELETE, MERGE
    contexts: List[str] = field(default_factory=list)  # 사용된 맥락


@dataclass
class StatementInfo:
    """SQL 문장 정보"""
    line_number: int
    statement_type: str  # SELECT, INSERT, UPDATE, DELETE, MERGE, etc.
    content: str
    tables: List[str] = field(default_factory=list)


@dataclass
class ProcedureInfo:
    """프로시저 정보"""
    name: str
    file_name: str
    folder_name: str
    project_name: str
    start_line: int
    end_line: int
    summary: str
    code: str
    summarized_code: str
    token_count: int
    has_children: bool
    
    # 분석 결과
    tables: Dict[str, TableInfo] = field(default_factory=dict)
    statements: List[StatementInfo] = field(default_factory=list)
    variables: List[str] = field(default_factory=list)
    cursors: List[str] = field(default_factory=list)
    called_procedures: List[str] = field(default_factory=list)


class ProcedureAnalyzer:
    """Stored Procedure 분석기"""
    
    # SQL 키워드 패턴 (analysis.py의 DML_STATEMENT_TYPES와 일치)
    DML_PATTERNS = {
        'SELECT': r'\bSELECT\b',
        'INSERT': r'\bINSERT\s+INTO\b',
        'UPDATE': r'\bUPDATE\b',
        'DELETE': r'\bDELETE\s+FROM\b',
        'MERGE': r'\bMERGE\s+INTO\b',
        'EXECUTE_IMMEDIATE': r'\bEXECUTE\s+IMMEDIATE\b',
        'FETCH': r'\bFETCH\b',
        'CREATE_TEMP_TABLE': r'\bCREATE\s+(?:TEMPORARY|TEMP)\s+TABLE\b',
        'CTE': r'\bWITH\s+[A-Z_][A-Z0-9_]*\s+AS\s*\(',
        'OPEN_CURSOR': r'\bOPEN\s+[A-Z_][A-Z0-9_]*\b'
    }
    
    # 테이블 추출 패턴
    TABLE_PATTERNS = {
        'FROM': r'\bFROM\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)',
        'INTO': r'\bINTO\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)',
        'UPDATE': r'\bUPDATE\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)',
        'JOIN': r'\bJOIN\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)',
        'MERGE_INTO': r'\bMERGE\s+INTO\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)',
    }
    
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.procedures: List[ProcedureInfo] = []
        
    def load_data(self):
        """JSON 데이터 로드"""
        with open(self.json_path, 'rb') as f:
            raw_content = f.read()
        
        content = raw_content.decode('utf-8', errors='ignore')
        if content.startswith('\ufeff'):
            content = content[1:]
        
        data = json.loads(content, strict=False)
        
        for record in data:
            if 'n' not in record:
                continue
                
            node = record['n']
            labels = node['labels']
            props = node['properties']
            
            # PROCEDURE 노드만 처리 (FUNCTION, PROCEDURE 포함)
            if 'PROCEDURE' not in labels:
                continue
                
            # 실제 프로시저만 처리 (함수 참조는 제외)
            if 'node_code' not in props:
                continue
            
            proc = ProcedureInfo(
                name=props.get('procedure_name', 'UNKNOWN'),
                file_name=props.get('file_name', ''),
                folder_name=props.get('folder_name', ''),
                project_name=props.get('project_name', ''),
                start_line=props.get('startLine', 0),
                end_line=props.get('endLine', 0),
                summary=props.get('summary', ''),
                code=props.get('node_code', ''),
                summarized_code=props.get('summarized_code', ''),
                token_count=props.get('token', 0),
                has_children=props.get('has_children', False)
            )
            
            self.procedures.append(proc)
    
    def analyze_procedures(self):
        """모든 프로시저 분석"""
        for proc in self.procedures:
            self._analyze_procedure(proc)
    
    def _analyze_procedure(self, proc: ProcedureInfo):
        """개별 프로시저 분석"""
        code = proc.code
        lines = code.split('\n')
        
        # 변수 추출
        proc.variables = self._extract_variables(code)
        
        # 커서 추출
        proc.cursors = self._extract_cursors(code)
        
        # 호출된 프로시저 추출
        proc.called_procedures = self._extract_called_procedures(code)
        
        # SQL 문장 분석
        self._analyze_statements(proc, lines)
    
    def _extract_variables(self, code: str) -> List[str]:
        """변수 선언 추출 (analysis.py의 VARIABLE_DECLARATION_TYPES와 유사하게 처리)
        
        주의: analysis.py는 LLM을 사용하므로 완전히 동일하지 않을 수 있음.
        정규식으로는 기본 타입만 추출 가능.
        """
        variables = []
        
        # Oracle 변수 선언 패턴 (더 많은 타입 포함)
        patterns = [
            # 기본 타입
            r'^\s*([A-Z_][A-Z0-9_]*)\s+VARCHAR2',
            r'^\s*([A-Z_][A-Z0-9_]*)\s+NUMBER',
            r'^\s*([A-Z_][A-Z0-9_]*)\s+DATE',
            r'^\s*([A-Z_][A-Z0-9_]*)\s+CHAR',
            r'^\s*([A-Z_][A-Z0-9_]*)\s+INTEGER',
            r'^\s*([A-Z_][A-Z0-9_]*)\s+BOOLEAN',
            # %TYPE, %ROWTYPE
            r'^\s*([A-Z_][A-Z0-9_]*)\s+[A-Z_][A-Z0-9_]*\s*%TYPE',
            r'^\s*([A-Z_][A-Z0-9_]*)\s+[A-Z_][A-Z0-9_]*\s*%ROWTYPE',
            # 파라미터 (IN, OUT, IN OUT)
            r'^\s*(?:IN|OUT|IN\s+OUT)\s+([A-Z_][A-Z0-9_]*)\s+',
        ]
        
        for line in code.split('\n'):
            for pattern in patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    variables.append(match.group(1))
        
        return list(set(variables))
    
    def _extract_cursors(self, code: str) -> List[str]:
        """커서 선언 추출 (analysis.py는 LLM을 사용하므로 완전히 동일하지 않을 수 있음)
        
        주의: analysis.py는 REF CURSOR, SYS_REFCURSOR 등도 처리하지만
        정규식으로는 명명형 커서만 추출 가능.
        """
        cursors = []
        
        # 명명형 커서 패턴
        patterns = [
            r'\bCURSOR\s+([A-Z_][A-Z0-9_]*)\s+IS',
            r'\bCURSOR\s+([A-Z_][A-Z0-9_]*)\s+FOR',
            # REF CURSOR 타입 변수 (간단한 패턴만)
            r'\b([A-Z_][A-Z0-9_]*)\s+(?:SYS_)?REF\s+CURSOR',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, code, re.IGNORECASE):
                cursors.append(match.group(1))
        
        return list(set(cursors))
    
    def _extract_called_procedures(self, code: str) -> List[str]:
        """호출된 프로시저/함수 추출 (analysis.py는 LLM을 사용하므로 완전히 동일하지 않을 수 있음)
        
        주의: analysis.py는 LLM이 프로시저 호출을 더 정확히 식별하지만,
        정규식으로는 기본 패턴만 추출 가능.
        """
        called = []
        
        # EXECUTE, CALL 패턴 (더 많은 패턴 포함)
        patterns = [
            # EXECUTE IMMEDIATE 내부의 호출
            r"EXECUTE\s+IMMEDIATE\s+['\"](?:CALL\s+)?([A-Z_][A-Z0-9_.]*)",
            # 직접 호출
            r'\bCALL\s+([A-Z_][A-Z0-9_.]*)',
            # 패키지.프로시저 형태
            r'\b([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)\s*\(',
            # 단독 프로시저 호출 (함수 호출과 구분 어려움)
            r'\b([A-Z_][A-Z0-9_]*)\s*\([^)]*\)\s*;',
            # DBMS_OUTPUT.PUT_LINE 등
            r'([A-Z_][A-Z0-9_.]*\.PUT_LINE)',
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, code, re.IGNORECASE):
                proc_name = match.group(1)
                # 괄호 제거 (analysis.py는 이름만 반환)
                proc_name = proc_name.split('(')[0].strip()
                if proc_name:
                    called.append(proc_name)
        
        return list(set(called))
    
    def _analyze_statements(self, proc: ProcedureInfo, lines: List[str]):
        """SQL 문장 분석 및 테이블 추출"""
        current_statement = []
        statement_start_line = 0
        in_statement = False
        
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            
            # 주석 제거
            if stripped.startswith('--') or stripped.startswith('/*'):
                continue
            
            # DML 문장 시작 감지
            for stmt_type, pattern in self.DML_PATTERNS.items():
                if re.search(pattern, line, re.IGNORECASE):
                    if current_statement and in_statement:
                        # 이전 문장 처리
                        self._process_statement(proc, current_statement, statement_start_line)
                    
                    current_statement = [line]
                    statement_start_line = i
                    in_statement = True
                    break
            else:
                if in_statement:
                    current_statement.append(line)
                    
                    # 문장 종료 감지 (세미콜론)
                    if ';' in line:
                        self._process_statement(proc, current_statement, statement_start_line)
                        current_statement = []
                        in_statement = False
        
        # 마지막 문장 처리
        if current_statement:
            self._process_statement(proc, current_statement, statement_start_line)
    
    def _process_statement(self, proc: ProcedureInfo, statement_lines: List[str], start_line: int):
        """SQL 문장 처리"""
        statement = ' '.join(statement_lines)
        
        # 문장 타입 결정
        stmt_type = 'UNKNOWN'
        for stype, pattern in self.DML_PATTERNS.items():
            if re.search(pattern, statement, re.IGNORECASE):
                stmt_type = stype
                break
        
        # 테이블 추출
        tables = self._extract_tables(statement, stmt_type)
        
        # Statement 정보 생성
        stmt_info = StatementInfo(
            line_number=start_line,
            statement_type=stmt_type,
            content=statement.strip(),
            tables=tables
        )
        proc.statements.append(stmt_info)
        
        # 프로시저의 테이블 정보 업데이트
        for table in tables:
            if table not in proc.tables:
                proc.tables[table] = TableInfo(name=table)
            
            proc.tables[table].operations.add(stmt_type)
            context = f"Line {start_line}: {stmt_type}"
            proc.tables[table].contexts.append(context)
    
    def _extract_tables(self, statement: str, stmt_type: str) -> List[str]:
        """SQL 문장에서 테이블 추출
        
        주의: analysis.py는 LLM을 사용하여 더 정확하게 테이블을 추출하지만,
        정규식으로는 기본 패턴만 추출 가능.
        스키마명은 유지하는 것이 analysis.py와 일치 (프롬프트에서 SCHEMA.TABLE_NAME 요구)
        """
        tables = set()
        
        for pattern_name, pattern in self.TABLE_PATTERNS.items():
            matches = re.finditer(pattern, statement, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1).strip()
                
                # 스키마명 유지 (analysis.py는 SCHEMA.TABLE_NAME 형식 사용)
                # 단, DB 링크(@)가 포함된 경우는 제외 (analysis.py는 dbLinks에 별도 저장)
                if '@' in table_name:
                    continue
                
                # 별칭이나 예약어 제외
                table_name_upper = table_name.upper()
                if table_name_upper in ['DUAL', 'X', 'Y', 'A', 'B', 'T']:
                    continue
                
                # CTE 별칭 제외 (WITH 절의 임시 결과 집합)
                # 정규식으로는 완벽히 구분 어려우므로 기본 필터만 적용
                tables.add(table_name)
        
        return sorted(list(tables))
    
    def generate_report(self, output_path: str = 'procedure_analysis_report.html'):
        """HTML 레포트 생성"""
        html_content = self._generate_html()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✓ 레포트 생성 완료: {output_path}")
    
    def _generate_html(self) -> str:
        """HTML 레포트 생성"""
        # 전체 통계
        total_procs = len(self.procedures)
        total_tables = set()
        total_statements = 0
        
        for proc in self.procedures:
            total_tables.update(proc.tables.keys())
            total_statements += len(proc.statements)
        
        html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stored Procedure 분석 레포트</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        .header .subtitle {{
            font-size: 1.2em;
            opacity: 0.9;
        }}
        
        .summary {{
            padding: 30px 40px;
            background: #f8f9fa;
            border-bottom: 3px solid #667eea;
        }}
        
        .summary h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.8em;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}
        
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
            border-left: 4px solid #667eea;
        }}
        
        .stat-card .number {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }}
        
        .stat-card .label {{
            color: #666;
            margin-top: 5px;
        }}
        
        .content {{
            padding: 40px;
        }}
        
        .procedure {{
            margin-bottom: 50px;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            overflow: hidden;
            transition: transform 0.3s ease;
        }}
        
        .procedure:hover {{
            transform: translateY(-5px);
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
        }}
        
        .procedure-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px 30px;
            cursor: pointer;
        }}
        
        .procedure-header h3 {{
            font-size: 1.5em;
            margin-bottom: 5px;
        }}
        
        .procedure-meta {{
            font-size: 0.9em;
            opacity: 0.9;
            margin-top: 10px;
        }}
        
        .procedure-body {{
            padding: 30px;
        }}
        
        .section {{
            margin-bottom: 30px;
        }}
        
        .section h4 {{
            color: #667eea;
            font-size: 1.3em;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #667eea;
        }}
        
        .summary-text {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #28a745;
            line-height: 1.8;
        }}
        
        .table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
            background: white;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            border-radius: 8px;
            overflow: hidden;
        }}
        
        .table thead {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }}
        
        .table th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
        }}
        
        .table td {{
            padding: 12px 15px;
            border-bottom: 1px solid #e0e0e0;
        }}
        
        .table tbody tr:hover {{
            background: #f8f9fa;
        }}
        
        .badge {{
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            margin-right: 5px;
            margin-bottom: 5px;
        }}
        
        .badge-select {{ background: #17a2b8; color: white; }}
        .badge-insert {{ background: #28a745; color: white; }}
        .badge-update {{ background: #ffc107; color: #333; }}
        .badge-delete {{ background: #dc3545; color: white; }}
        .badge-merge {{ background: #6f42c1; color: white; }}
        
        .code-block {{
            background: #282c34;
            color: #abb2bf;
            padding: 20px;
            border-radius: 8px;
            overflow-x: auto;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            line-height: 1.5;
            margin-top: 10px;
        }}
        
        .list {{
            list-style: none;
            padding-left: 0;
        }}
        
        .list li {{
            padding: 8px 12px;
            margin-bottom: 5px;
            background: #f8f9fa;
            border-radius: 5px;
            border-left: 3px solid #667eea;
        }}
        
        .toc {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
        }}
        
        .toc h3 {{
            color: #667eea;
            margin-bottom: 15px;
        }}
        
        .toc ul {{
            list-style: none;
        }}
        
        .toc li {{
            padding: 8px 0;
        }}
        
        .toc a {{
            color: #667eea;
            text-decoration: none;
            transition: color 0.3s;
        }}
        
        .toc a:hover {{
            color: #764ba2;
            text-decoration: underline;
        }}
        
        .footer {{
            background: #282c34;
            color: white;
            text-align: center;
            padding: 20px;
            margin-top: 40px;
        }}
        
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            
            .container {{
                box-shadow: none;
            }}
            
            .procedure {{
                page-break-inside: avoid;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Stored Procedure 분석 레포트</h1>
            <div class="subtitle">Neo4j 데이터 기반 상세 분석 결과</div>
        </div>
        
        <div class="summary">
            <h2>📈 전체 요약</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="number">{total_procs}</div>
                    <div class="label">총 프로시저 수</div>
                </div>
                <div class="stat-card">
                    <div class="number">{len(total_tables)}</div>
                    <div class="label">사용된 테이블 수</div>
                </div>
                <div class="stat-card">
                    <div class="number">{total_statements}</div>
                    <div class="label">총 SQL 문장 수</div>
                </div>
                <div class="stat-card">
                    <div class="number">{sum(proc.token_count for proc in self.procedures):,}</div>
                    <div class="label">총 토큰 수</div>
                </div>
            </div>
        </div>
        
        <div class="content">
            <div class="toc">
                <h3>📑 목차</h3>
                <ul>
"""
        
        # 목차 생성
        for i, proc in enumerate(self.procedures, 1):
            html += f'                    <li><a href="#proc-{i}">{i}. {html_escape(proc.name)}</a></li>\n'
        
        html += """                </ul>
            </div>
"""
        
        # 각 프로시저 상세 정보
        for i, proc in enumerate(self.procedures, 1):
            html += self._generate_procedure_section(proc, i)
        
        html += """        </div>
        
        <div class="footer">
            <p>Generated by Stored Procedure Analyzer | Neo4j Data Analysis Tool</p>
        </div>
    </div>
</body>
</html>"""
        
        return html
    
    def _generate_procedure_section(self, proc: ProcedureInfo, index: int) -> str:
        """개별 프로시저 섹션 생성"""
        html = f"""
            <div class="procedure" id="proc-{index}">
                <div class="procedure-header">
                    <h3>{index}. {html_escape(proc.name)}</h3>
                    <div class="procedure-meta">
                        📁 파일: {html_escape(proc.file_name)} | 
                        📂 폴더: {html_escape(proc.folder_name)} | 
                        📦 프로젝트: {html_escape(proc.project_name)} | 
                        📏 라인: {proc.start_line}-{proc.end_line} ({proc.end_line - proc.start_line + 1} lines)
                    </div>
                </div>
                
                <div class="procedure-body">
"""
        
        # 요약
        if proc.summary:
            html += f"""
                    <div class="section">
                        <h4>📝 요약</h4>
                        <div class="summary-text">
                            {html_escape(proc.summary)}
                        </div>
                    </div>
"""
        
        # 사용된 테이블
        if proc.tables:
            html += """
                    <div class="section">
                        <h4>🗄️ 사용된 테이블</h4>
                        <table class="table">
                            <thead>
                                <tr>
                                    <th>테이블명</th>
                                    <th>작업 유형</th>
                                    <th>사용 횟수</th>
                                    <th>상세 위치</th>
                                </tr>
                            </thead>
                            <tbody>
"""
            for table_name, table_info in sorted(proc.tables.items()):
                operations_html = ''
                for op in sorted(table_info.operations):
                    badge_class = f"badge-{op.lower()}"
                    operations_html += f'<span class="badge {badge_class}">{op}</span>'
                
                contexts_html = '<br>'.join(html_escape(ctx) for ctx in table_info.contexts[:5])
                if len(table_info.contexts) > 5:
                    contexts_html += f'<br>... 외 {len(table_info.contexts) - 5}개'
                
                html += f"""
                                <tr>
                                    <td><strong>{html_escape(table_name)}</strong></td>
                                    <td>{operations_html}</td>
                                    <td>{len(table_info.contexts)}</td>
                                    <td style="font-size: 0.85em;">{contexts_html}</td>
                                </tr>
"""
            html += """
                            </tbody>
                        </table>
                    </div>
"""
        
        # SQL 문장 분석
        if proc.statements:
            html += """
                    <div class="section">
                        <h4>🔍 SQL 문장 분석</h4>
                        <table class="table">
                            <thead>
                                <tr>
                                    <th style="width: 80px;">라인</th>
                                    <th style="width: 100px;">타입</th>
                                    <th>관련 테이블</th>
                                    <th>내용 (일부)</th>
                                </tr>
                            </thead>
                            <tbody>
"""
            for stmt in proc.statements[:20]:  # 최대 20개만 표시
                badge_class = f"badge-{stmt.statement_type.lower()}"
                tables_str = ', '.join(stmt.tables) if stmt.tables else '-'
                
                # 문장 내용을 100자로 제한
                content_preview = stmt.content[:100].replace('\n', ' ').strip()
                if len(stmt.content) > 100:
                    content_preview += '...'
                
                html += f"""
                                <tr>
                                    <td>{stmt.line_number}</td>
                                    <td><span class="badge {badge_class}">{stmt.statement_type}</span></td>
                                    <td style="font-size: 0.9em;">{html_escape(tables_str)}</td>
                                    <td style="font-size: 0.85em;">{html_escape(content_preview)}</td>
                                </tr>
"""
            
            if len(proc.statements) > 20:
                html += f"""
                                <tr>
                                    <td colspan="4" style="text-align: center; color: #666;">
                                        ... 외 {len(proc.statements) - 20}개의 SQL 문장
                                    </td>
                                </tr>
"""
            
            html += """
                            </tbody>
                        </table>
                    </div>
"""
        
        # 변수 및 커서
        if proc.variables or proc.cursors:
            html += """
                    <div class="section">
                        <h4>🔧 변수 및 커서</h4>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
"""
            
            if proc.variables:
                html += f"""
                            <div>
                                <strong>변수 ({len(proc.variables)}개):</strong>
                                <ul class="list">
"""
                for var in sorted(proc.variables)[:10]:
                    html += f'                                    <li>{html_escape(var)}</li>\n'
                if len(proc.variables) > 10:
                    html += f'                                    <li>... 외 {len(proc.variables) - 10}개</li>\n'
                html += """                                </ul>
                            </div>
"""
            
            if proc.cursors:
                html += f"""
                            <div>
                                <strong>커서 ({len(proc.cursors)}개):</strong>
                                <ul class="list">
"""
                for cursor in sorted(proc.cursors):
                    html += f'                                    <li>{html_escape(cursor)}</li>\n'
                html += """                                </ul>
                            </div>
"""
            
            html += """
                        </div>
                    </div>
"""
        
        # 호출된 프로시저
        if proc.called_procedures:
            html += f"""
                    <div class="section">
                        <h4>📞 호출된 프로시저/함수 ({len(proc.called_procedures)}개)</h4>
                        <ul class="list">
"""
            for called in sorted(proc.called_procedures):
                html += f'                            <li>{html_escape(called)}</li>\n'
            html += """                        </ul>
                    </div>
"""
        
        # 코드 미리보기 (요약된 코드)
        if proc.summarized_code:
            preview = proc.summarized_code[:1000]
            if len(proc.summarized_code) > 1000:
                preview += '\n... (생략)'
            
            html += f"""
                    <div class="section">
                        <h4>💻 코드 미리보기</h4>
                        <div class="code-block">{html_escape(preview)}</div>
                    </div>
"""
        
        html += """
                </div>
            </div>
"""
        
        return html


def main():
    """메인 함수"""
    print("=" * 80)
    print("Stored Procedure 분석 프로그램")
    print("=" * 80)
    print()
    
    # 분석기 초기화
    analyzer = ProcedureAnalyzer('test/data/neo4j_exports/records.json')
    
    # 데이터 로드
    print("📂 데이터 로딩 중...")
    analyzer.load_data()
    print(f"✓ {len(analyzer.procedures)}개의 프로시저를 로드했습니다.")
    print()
    
    # 프로시저 분석
    print("🔍 프로시저 분석 중...")
    analyzer.analyze_procedures()
    
    # 분석 결과 요약
    total_tables = set()
    total_statements = 0
    total_variables = 0
    total_cursors = 0
    
    for proc in analyzer.procedures:
        total_tables.update(proc.tables.keys())
        total_statements += len(proc.statements)
        total_variables += len(proc.variables)
        total_cursors += len(proc.cursors)
    
    print(f"✓ 분석 완료")
    print(f"  - 총 프로시저: {len(analyzer.procedures)}개")
    print(f"  - 사용된 테이블: {len(total_tables)}개")
    print(f"  - SQL 문장: {total_statements}개")
    print(f"  - 변수: {total_variables}개")
    print(f"  - 커서: {total_cursors}개")
    print()
    
    # 레포트 생성
    print("📊 HTML 레포트 생성 중...")
    output_path = 'test/data/procedure_analysis_report.html'
    analyzer.generate_report(output_path)
    print()
    
    # 프로시저별 상세 정보 출력
    print("=" * 80)
    print("프로시저별 상세 분석 결과")
    print("=" * 80)
    print()
    
    for i, proc in enumerate(analyzer.procedures, 1):
        print(f"{i}. {proc.name}")
        print(f"   파일: {proc.file_name}")
        print(f"   라인: {proc.start_line}-{proc.end_line}")
        print(f"   테이블: {len(proc.tables)}개 - {', '.join(sorted(proc.tables.keys()))}")
        print(f"   SQL 문장: {len(proc.statements)}개")
        
        if proc.summary:
            summary_preview = proc.summary[:150]
            if len(proc.summary) > 150:
                summary_preview += '...'
            print(f"   요약: {summary_preview}")
        
        print()


if __name__ == '__main__':
    main()

