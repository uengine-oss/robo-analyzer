#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stored Procedure 분석 및 레포트 생성 프로그램
Neo4j에서 추출한 구조화된 JSON 데이터를 분석하여 상세 레포트를 생성합니다.
"""

import json
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
    directory: str
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
    """Stored Procedure 분석기 (구조화된 JSON 기반)"""
    
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.procedures: List[ProcedureInfo] = []
        
    def load_data(self):
        """구조화된 JSON 데이터 로드 및 프로시저별 그룹화"""
        with open(self.json_path, 'rb') as f:
            raw_content = f.read()
        
        content = raw_content.decode('utf-8', errors='ignore')
        if content.startswith('\ufeff'):
            content = content[1:]
        
        data = json.loads(content, strict=False)
        
        # procedure_name과 procedure_start_line으로 그룹화
        procedure_groups = defaultdict(list)
        for record in data:
            proc_name = record.get('procedure_name')
            proc_start_line = record.get('procedure_start_line')
            
            if proc_name is not None and proc_start_line is not None:
                key = (proc_name, proc_start_line)
                procedure_groups[key].append(record)
        
        # 각 프로시저 그룹 처리
        for (proc_name, proc_start_line), records in procedure_groups.items():
            proc = self._process_procedure_group(records)
            if proc:
                self.procedures.append(proc)
    
    def _process_procedure_group(self, records: List[Dict]) -> Optional[ProcedureInfo]:
        """프로시저 그룹 처리"""
        # PROCEDURE 노드 찾기
        procedure_node = None
        for record in records:
            if record.get('node_type') == 'PROCEDURE':
                procedure_node = record.get('procedure_node')
                break
        
        if not procedure_node:
            return None
        
        props = procedure_node.get('properties', {})
        
        # 프로시저 기본 정보
        proc = ProcedureInfo(
            name=props.get('procedure_name', 'UNKNOWN'),
            file_name=props.get('file_name', ''),
            directory=props.get('directory', ''),
            project_name=props.get('project_name', ''),
            start_line=props.get('startLine', 0),
            end_line=props.get('endLine', 0),
            summary=props.get('summary', ''),
            code=props.get('node_code', ''),
            summarized_code=props.get('summarized_code', ''),
            token_count=props.get('token', 0),
            has_children=props.get('has_children', False)
        )
        
        # 각 레코드 분석
        for record in records:
            node_type = record.get('node_type')
            node1 = record.get('node1')
            node2 = record.get('node2')
            relationship = record.get('relationship')
            
            if not node1:
                continue
            
            # DML 노드 처리 (SELECT, INSERT, UPDATE, DELETE, MERGE 등)
            if node_type in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 
                           'EXECUTE_IMMEDIATE', 'FETCH', 'CTE', 'OPEN_CURSOR']:
                self._process_dml_node(proc, node1, node2, relationship, node_type)
            
            # CREATE_TEMP_TABLE 노드 처리
            elif node_type == 'CREATE_TEMP_TABLE':
                self._process_temp_table_node(proc, node1)
            
            # Variable 노드 처리 (SPEC/DECLARE를 통해)
            elif node_type in ['SPEC', 'DECLARE', 'PACKAGE_VARIABLE']:
                if node2 and 'Variable' in node2.get('labels', []):
                    self._process_variable_node(proc, node2)
            
            # CALL 관계 처리
            if relationship and relationship.get('type') == 'CALL':
                if node2:
                    node2_labels = node2.get('labels', [])
                    if 'PROCEDURE' in node2_labels or 'FUNCTION' in node2_labels:
                        node2_props = node2.get('properties', {})
                        called_name = node2_props.get('procedure_name') or node2_props.get('name', 'UNKNOWN')
                        if called_name not in proc.called_procedures:
                            proc.called_procedures.append(called_name)
        
        return proc
    
    def _process_dml_node(self, proc: ProcedureInfo, dml_node: Dict, 
                         table_node: Optional[Dict], relationship: Optional[Dict],
                         node_type: str):
        """DML 노드 처리"""
        dml_props = dml_node.get('properties', {})
        
        # Statement 정보 생성
        start_line = dml_props.get('startLine', 0)
        node_code = dml_props.get('node_code', '')
        
        stmt_info = StatementInfo(
            line_number=start_line,
            statement_type=node_type,
            content=node_code,
            tables=[]
        )
        
        # 테이블 정보 추출
        if table_node and 'Table' in table_node.get('labels', []):
            table_props = table_node.get('properties', {})
            table_name = table_props.get('name', '')
            
            if table_name:
                stmt_info.tables.append(table_name)
                
                # 프로시저의 테이블 정보 업데이트
                if table_name not in proc.tables:
                    proc.tables[table_name] = TableInfo(name=table_name)
                
                # 관계 타입에 따라 작업 유형 결정
                if relationship:
                    rel_type = relationship.get('type', '')
                    if rel_type == 'WRITES':
                        proc.tables[table_name].operations.add(node_type)
                    elif rel_type == 'FROM':
                        proc.tables[table_name].operations.add(node_type)
                
                context = f"Line {start_line}: {node_type}"
                proc.tables[table_name].contexts.append(context)
        
        proc.statements.append(stmt_info)
    
    def _process_temp_table_node(self, proc: ProcedureInfo, temp_table_node: Dict):
        """CREATE_TEMP_TABLE 노드 처리"""
        temp_table_props = temp_table_node.get('properties', {})
        table_name = temp_table_props.get('name', '')
        
        if table_name:
            if table_name not in proc.tables:
                proc.tables[table_name] = TableInfo(name=table_name)
            
            proc.tables[table_name].operations.add('CREATE_TEMP_TABLE')
            start_line = temp_table_props.get('startLine', 0)
            context = f"Line {start_line}: CREATE_TEMP_TABLE"
            proc.tables[table_name].contexts.append(context)
            
            # Statement 정보도 추가
            stmt_info = StatementInfo(
                line_number=start_line,
                statement_type='CREATE_TEMP_TABLE',
                content=temp_table_props.get('node_code', ''),
                tables=[table_name]
            )
            proc.statements.append(stmt_info)
    
    def _process_variable_node(self, proc: ProcedureInfo, variable_node: Dict):
        """Variable 노드 처리"""
        var_props = variable_node.get('properties', {})
        var_name = var_props.get('name', '')
        
        if var_name and var_name not in proc.variables:
            proc.variables.append(var_name)
    
    def analyze_procedures(self):
        """모든 프로시저 분석 (이미 load_data에서 처리됨)"""
        # load_data에서 이미 모든 분석이 완료되므로 여기서는 정렬만 수행
        for proc in self.procedures:
            # 변수, 커서, 호출된 프로시저 정렬
            proc.variables.sort()
            proc.cursors.sort()
            proc.called_procedures.sort()
            
            # statements 정렬 (라인 번호 기준)
            proc.statements.sort(key=lambda x: x.line_number)
    
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
        .badge-create_temp_table {{ background: #fd7e14; color: white; }}
        .badge-execute_immediate {{ background: #20c997; color: white; }}
        .badge-fetch {{ background: #6c757d; color: white; }}
        .badge-cte {{ background: #e83e8c; color: white; }}
        .badge-open_cursor {{ background: #6610f2; color: white; }}
        
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
            <div class="subtitle">Neo4j 그래프 데이터 기반 상세 분석 결과</div>
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
            <p>Generated by Stored Procedure Analyzer | Neo4j Graph Data Analysis Tool</p>
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
                        📂 디렉토리: {html_escape(proc.directory)} | 
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
                    badge_class = f"badge-{op.lower().replace('_', '_')}"
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
                badge_class = f"badge-{stmt.statement_type.lower().replace('_', '_')}"
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
    print("Stored Procedure 분석 프로그램 (구조화된 JSON 기반)")
    print("=" * 80)
    print()
    
    # 분석기 초기화
    analyzer = ProcedureAnalyzer('test/data/neo4j_exports/records.json')
    
    # 데이터 로드 및 분석
    print("📂 데이터 로딩 및 분석 중...")
    analyzer.load_data()
    analyzer.analyze_procedures()
    print(f"✓ {len(analyzer.procedures)}개의 프로시저를 분석했습니다.")
    print()
    
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


def test_procedure_analyzer():
    """pytest용 테스트 함수"""
    analyzer = ProcedureAnalyzer('test/data/neo4j_exports/records.json')
    
    # 데이터 로드 및 분석
    analyzer.load_data()
    analyzer.analyze_procedures()
    
    # 기본 검증
    assert len(analyzer.procedures) > 0, "프로시저가 로드되지 않았습니다"
    
    # 첫 번째 프로시저 검증
    proc = analyzer.procedures[0]
    assert proc.name, "프로시저 이름이 없습니다"
    assert proc.start_line > 0, "프로시저 시작 라인이 없습니다"
    
    # 레포트 생성 테스트
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        temp_path = f.name
    
    try:
        analyzer.generate_report(temp_path)
        assert os.path.exists(temp_path), "HTML 레포트가 생성되지 않았습니다"
        assert os.path.getsize(temp_path) > 0, "HTML 레포트가 비어있습니다"
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    
    print(f"✓ 테스트 통과: {len(analyzer.procedures)}개 프로시저 분석 완료")


if __name__ == '__main__':
    main()
