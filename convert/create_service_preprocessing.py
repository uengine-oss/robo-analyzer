import logging
import textwrap
from prompt.convert_service_prompt import convert_service_code, convert_exception_code
from prompt.convert_summarized_service_prompt import convert_summarized_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import extract_used_query_methods, collect_variables_in_range, build_java_base_path, save_file, convert_to_pascal_case


# ----- 상수 정의 -----
TOKEN_THRESHOLD = 1000
CODE_PLACEHOLDER = "...code..."
DML_TYPES = frozenset(["SELECT", "INSERT", "UPDATE", "DELETE", "FETCH", "MERGE", "JOIN", "ALL_UNION", "UNION"])


# ----- 서비스 전처리 클래스 -----
class ServicePreprocessingGenerator:
    """
    서비스 전처리 전체 라이프사이클 관리
    - 단일 컨텍스트 누적 방식으로 자바 코드 생성
    - 대용량 부모(토큰≥1000, 자식 보유) 스켈레톤 관리
    - 토큰 임계 도달 시 LLM 분석 수행
    """
    __slots__ = (
        'traverse_nodes', 'variable_nodes', 'command_class_variable', 'service_skeleton',
        'query_method_list', 'folder_name', 'file_name', 'procedure_name', 'sequence_methods',
        'user_id', 'api_key', 'locale', 'project_name',
        'merged_java_code', 'total_tokens', 'tracking_variables', 'current_parent', 
        'java_buffer', 'sp_code_parts', 'sp_start', 'sp_end',
        'pending_try_mode'
    )

    def __init__(self, traverse_nodes: list, variable_nodes: list, command_class_variable: dict,
                 service_skeleton: str, query_method_list: dict, folder_name: str, file_name: str,
                 procedure_name: str, sequence_methods: list, user_id: str, api_key: str, locale: str, project_name: str = "demo"):
        self.traverse_nodes = traverse_nodes
        self.variable_nodes = variable_nodes
        self.command_class_variable = command_class_variable
        self.service_skeleton = service_skeleton
        self.query_method_list = query_method_list
        self.folder_name = folder_name
        self.file_name = file_name
        self.procedure_name = procedure_name
        self.sequence_methods = sequence_methods
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name or "demo"

        # 상태 초기화
        self.merged_java_code = ""
        self.total_tokens = int(0)  # 명시적 int 타입
        self.tracking_variables = {}
        self.current_parent = None
        self.java_buffer = ""
        self.sp_code_parts = []  # 문자열 연결 최적화
        self.sp_start = None
        self.sp_end = None
        
        # TRY-EXCEPTION 처리
        self.pending_try_mode = False

    # ----- 공개 메서드 -----

    async def generate(self) -> str:
        """
        전체 노드를 순회하며 자바 코드 생성
        
        Returns:
            str: 최종 병합된 자바 코드
        """
        logging.info(f"📋 노드 순회 시작")

        # 🎯 중복 제거: 같은 라인 범위는 한 번만 처리
        seen_nodes = set()
        node_count = 0
        for record in self.traverse_nodes:
            node = record['n']
            node_key = (node.get('startLine'), node.get('endLine'))
            if node_key in seen_nodes:
                continue
            seen_nodes.add(node_key)
            node_count += 1
            await self._process_node(record)

        await self._finalize_remaining()

        logging.info(f"✅ 총 {node_count}개 노드 처리 완료\n")
        return self.merged_java_code.strip()

    # ----- 노드 처리 -----

    async def _process_node(self, record: dict) -> None:
        """단일 노드 처리"""
        node = record['n']
        # Neo4j labels() 함수로 가져온 레이블 사용
        node_labels = record.get('nodeLabels', [])
        node_type = node_labels[0] if node_labels else node.get('name', 'UNKNOWN')
        has_children = bool(node.get('has_children', False))
        token = int(node.get('token', 0) or 0)
        start_line = int(node.get('startLine', 0) or 0)
        end_line = int(node.get('endLine', 0) or 0)
        relationship = record['r'][1] if record.get('r') else 'NEXT'

        # 노드 처리 로그 (간결하게)
        name = node_type.split('[')[0] if '[' in str(node_type) else str(node_type)
        depth = "  " if self.current_parent else ""
        logging.debug(f"{depth}→ {name}[{start_line}~{end_line}] 토큰={token}")

        # 🚀 TRY 노드 감지 → 플래그 설정
        if node_type == 'TRY':
            self.pending_try_mode = True
            logging.info(f"  🔒 TRY 노드 감지 → EXCEPTION까지 merge 보류")
        
        # 🚀 EXCEPTION 노드 감지 → 전용 처리
        if node_type == 'EXCEPTION':
            await self._handle_exception_node(node, start_line, end_line)
            return  # EXCEPTION 처리 완료, 다음 노드로
        
        # 부모 경계 체크
        parent = self.current_parent
        if parent and relationship == 'NEXT' and start_line > parent['end']:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            await self._finalize_parent()

        # 노드 타입별 처리
        if token >= TOKEN_THRESHOLD and has_children and node_type not in DML_TYPES:
            # 큰 노드 처리 전에 쌓인 작은 노드들 먼저 변환
            if self.sp_code_parts:
                await self._analyze_and_merge()
            
            logging.info(f"  ┌─ 큰 노드 진입 [{start_line}~{end_line}] (토큰: {token})")
            await self._handle_large_node(node, start_line, end_line, token)
        else:
            self._handle_small_node(node, start_line, end_line, token)

        # 임계값 체크
        if int(self.total_tokens) >= TOKEN_THRESHOLD:
            logging.info(f"  ⚠️  토큰 임계값 도달 ({int(self.total_tokens)}) → LLM 분석 실행")
            await self._analyze_and_merge()

    # ----- 대용량 노드 처리 -----

    async def _handle_large_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        """대용량 노드(자식 있음, 토큰≥1000) 처리"""
        summarized = (node.get('summarized_code') or '').strip()
        if not summarized:
            return
        

        # 현재 컨텍스트 수집
        used_vars, used_queries = await self._collect_current_context()

        # LLM으로 스켈레톤 생성
        result = convert_summarized_code(
            summarized,
            self.service_skeleton,
            used_vars,
            self.command_class_variable,
            used_queries,
            self.sequence_methods,
            self.api_key,
            self.locale
        )
        skeleton = result['code']

        # 부모 설정 또는 삽입
        if not self.current_parent:
            self.current_parent = {'start': start_line, 'end': end_line, 'code': skeleton}
            logging.info(f"  │  부모 설정 완료 → 자식 노드 처리 시작")
        else:
            self.current_parent['code'] = self.current_parent['code'].replace(
                CODE_PLACEHOLDER, f"\n{textwrap.indent(skeleton, '    ')}", 1
            )
            logging.info(f"  │  중첩 부모에 삽입 완료")


    # ----- 소형 노드 처리 -----

    def _handle_small_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        """소형 노드 또는 리프 노드 처리"""
        node_code = (node.get('node_code') or '').strip()
        if not node_code:
            return

        # SP 코드 누적
        self.sp_code_parts.append(node_code)
        self.total_tokens = int(self.total_tokens) + int(token)

        # 범위 업데이트
        if self.sp_start is None or start_line < self.sp_start:
            self.sp_start = start_line
        if self.sp_end is None or end_line > self.sp_end:
            self.sp_end = end_line

    # ----- 변수/JPA 수집 -----

    async def _collect_current_context(self) -> tuple:
        """현재 사용 중인 변수와 쿼리 메서드 수집"""
        if not self.sp_start:
            return [], {}

        used_vars = []
        used_queries = {}

        # 변수 수집
        if self.variable_nodes:
            try:
                collected = await collect_variables_in_range(
                    self.variable_nodes, self.sp_start, self.sp_end or self.sp_start
                )
                used_vars = [{**v, 'role': self.tracking_variables.get(v['name'], '')} for v in collected]
            except Exception as e:
                logging.debug(f"변수 수집 스킵: {e}")

        # JPA 메서드 수집
        if self.query_method_list:
            try:
                used_queries = await extract_used_query_methods(
                    self.sp_start, self.sp_end or self.sp_start, self.query_method_list, {}
                )
            except Exception as e:
                logging.debug(f"JPA 수집 스킵: {e}")

        return used_vars, used_queries

    # ----- 부모 관리 -----

    async def _finalize_parent(self) -> None:
        """현재 부모 마무리"""
        if not self.current_parent:
            return
        
        logging.info(f"  └─ 큰 노드 완료 [{self.current_parent['start']}~{self.current_parent['end']}]")

        # 버퍼 삽입
        if self.java_buffer:
            self.current_parent['code'] = self.current_parent['code'].replace(
                CODE_PLACEHOLDER, f"\n{textwrap.indent(self.java_buffer.strip(), '    ')}", 1
            )

        # 병합 (TRY 대기 중이면 보류)
        if not self.pending_try_mode:
            self.merged_java_code += f"\n{self.current_parent['code']}"
            logging.info(f"     ✓ 부모 노드 병합 완료")
        else:
            logging.info(f"     ✓ TRY 부모 완료 (java_buffer 보관, EXCEPTION 대기)")

        # 초기화
        self.current_parent = None
        self.java_buffer = ""

    # ----- EXCEPTION 노드 전용 처리 -----

    async def _handle_exception_node(self, node: dict, start_line: int, end_line: int) -> None:
        """EXCEPTION 노드 전용 처리: 전체 코드를 try-catch로 감싸는 예외처리 구조 생성
        
        처리 흐름:
        1. TRY 노드 존재: TRY 블록 코드만 예외처리로 감싸기
        2. TRY 노드 미존재: 전체 메서드 코드를 예외처리로 감싸기
        
        Args:
            node: EXCEPTION 노드 데이터
            start_line: 시작 라인
            end_line: 종료 라인
        """
        logging.info(f"  ⚡ EXCEPTION 노드 감지 → 예외처리 구조 생성 시작")
        
        # 1. 쌓인 코드 먼저 분석
        if self.sp_code_parts:
            await self._analyze_and_merge()
        
        # 2. EXCEPTION 블록을 Java try-catch 구조로 변환
        node_code = (node.get('node_code') or '').strip()
        if not node_code:
            logging.warning(f"     ⚠️  EXCEPTION 노드 코드가 비어있음")
            return
            
        result = convert_exception_code(node_code, self.api_key, self.locale)
        exception_java_code = result.get('code', '').strip()
        
        if 'CodePlaceHolder' not in exception_java_code:
            logging.warning(f"     ⚠️  try-catch 템플릿에 CodePlaceHolder가 없음")
            return
        
        # 3. 전체 코드를 예외처리로 감싸기
        if self.pending_try_mode:
            # Case 1: TRY 노드 존재 → TRY 블록 코드만 감싸기
            try_block_code = self.java_buffer.strip()
            wrapped_code = exception_java_code.replace('CodePlaceHolder', try_block_code)
            self.merged_java_code += f"\n{wrapped_code}"
            logging.info(f"     ✓ TRY 블록 코드를 예외처리로 감쌈 (java_buffer 사용)")
        else:
            # Case 2: TRY 노드 미존재 → 전체 메서드 코드를 감싸기
            entire_code = self.merged_java_code.strip()
            wrapped_code = exception_java_code.replace('CodePlaceHolder', entire_code)
            self.merged_java_code = wrapped_code
            logging.info(f"     ✓ 전체 메서드 코드를 예외처리로 감쌈 (merged_java_code 사용)")
        
        # 4. 상태 초기화
        self.java_buffer = ""
        self.pending_try_mode = False
        logging.info(f"     ✓ 예외처리 완료 및 상태 초기화")

    # ----- 분석 및 병합 -----

    async def _analyze_and_merge(self) -> None:
        """LLM 분석 및 자바 코드 병합"""
        if not self.sp_code_parts or self.sp_start is None:
            return

        # 문자열 조인
        sp_code = '\n'.join(self.sp_code_parts)
        target = "부모버퍼" if self.current_parent else "최종코드"
        logging.info(f"  🤖 LLM 분석 시작: [{self.sp_start}~{self.sp_end}] {len(self.sp_code_parts)}개 파트 (토큰: {self.total_tokens})")

        # 변수 수집
        used_variables = []
        try:
            collected = await collect_variables_in_range(self.variable_nodes, self.sp_start, self.sp_end)
            used_variables = [{**v, 'role': self.tracking_variables.get(v['name'], '')} for v in collected]
        except Exception as e:
            logging.debug(f"변수 수집 스킵: {e}")

        # JPA 메서드 수집
        used_query_methods = {}
        try:
            used_query_methods = await extract_used_query_methods(
                self.sp_start, self.sp_end, self.query_method_list, {}
            )
        except Exception as e:
            logging.debug(f"JPA 수집 스킵: {e}")

        # LLM 분석 (일반 프롬프트만 사용)
        result = convert_service_code(
            sp_code,
            self.service_skeleton,
            used_variables,
            self.command_class_variable,
            used_query_methods,
            self.sequence_methods,
            self.api_key,
            self.locale,
            self.current_parent['code'] if self.current_parent else ""
        )

        # 변수 추적 업데이트
        self.tracking_variables.update(result['analysis'].get('variables', {}))

        # 생성된 자바 코드 병합
        java_code = (result.get('analysis', {}).get('code') or '').strip()
        if java_code:
            if self.current_parent:
                # 큰 노드 → java_buffer에 추가
                self.java_buffer += f"\n{java_code}"
                logging.info(f"     ✓ {target}에 추가")
            else:
                # 작은 노드 처리
                if self.pending_try_mode:
                    # TRY 노드 → java_buffer에 보관 (merge 안 함)
                    self.java_buffer += f"\n{java_code}"
                    logging.info(f"     ✓ TRY 코드 보관 → EXCEPTION 대기")
                else:
                    # 일반 노드 → 바로 merge
                    self.merged_java_code += f"\n{java_code}"
                    logging.info(f"     ✓ {target}에 추가")

        # 상태 초기화
        self.total_tokens = int(0)  # 명시적 int 타입
        self.sp_code_parts.clear()
        self.sp_start = None
        self.sp_end = None

    # ----- 마무리 -----

    async def _finalize_remaining(self) -> None:
        """남은 데이터 정리"""
        if self.current_parent:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            await self._finalize_parent()
        elif self.sp_code_parts:
            await self._analyze_and_merge()

    async def _save_service_file(self, service_class_name: str) -> str:
        """성능 최적화된 서비스 파일 자동 저장"""
        try:
            # 병합된 Java 코드를 서비스 스켈레톤에 삽입
            completed_service_code = self.service_skeleton.replace("CodePlaceHolder", self.merged_java_code.strip())
            
            # 저장 경로 설정 (최적화: 한 번만 계산)
            base_path = build_java_base_path(self.project_name, self.user_id, 'service')
            
            # 파일 저장 (비동기 최적화)
            await save_file(
                content=completed_service_code,
                filename=f"{service_class_name}.java",
                base_path=base_path
            )
            
            logging.info(f"✅ [{service_class_name}] 서비스 파일 자동 저장 완료")
            logging.info(f"📁 저장 경로: {base_path}/{service_class_name}.java")
            
            return completed_service_code
            
        except Exception as e:
            logging.error(f"❌ 서비스 파일 저장 실패: {str(e)}")
            raise ConvertingError(f"서비스 파일 저장 중 오류: {str(e)}")


# ----- 진입점 함수 -----
async def start_service_preprocessing(
    service_skeleton: str,
    command_class_variable: dict,
    procedure_name: str,
    query_method_list: dict,
    folder_name: str,
    file_name: str,
    sequence_methods: list,
    project_name: str,
    user_id: str,
    api_key: str,
    locale: str
) -> tuple:
    """
    서비스 전처리 시작
    
    Args:
        service_skeleton: 서비스 메서드 스켈레톤 템플릿
        command_class_variable: 커맨드 클래스 필드 정의
        procedure_name: 프로시저 이름
        query_method_list: JPA 쿼리 메서드 목록
        folder_name: 폴더명
        file_name: 파일명
        sequence_methods: 시퀀스 메서드 목록
        user_id: 사용자 ID
        api_key: LLM API 키
        locale: 로케일
    
    Returns:
        None (파일 내부에서 자동 저장)
    
    Raises:
        ConvertingError: 전처리 중 오류 발생 시
    """
    connection = Neo4jConnection()
    
    logging.info("\n" + "="*80)
    logging.info(f"⚙️  STEP 4: Service 코드 생성 - {procedure_name}")
    logging.info("="*80)
    logging.info(f"📁 파일: {folder_name}/{file_name}")

    try:
        # Neo4j 쿼리
        service_nodes, variable_nodes = await connection.execute_queries([
            f"""
            MATCH (p:PROCEDURE {{
              folder_name: '{folder_name}',
              file_name: '{file_name}',
              procedure_name: '{procedure_name}',
              user_id: '{user_id}'
            }})
            
            CALL {{
              WITH p
              MATCH (p)-[:PARENT_OF]->(c)
              WHERE NOT c:DECLARE AND NOT c:Table AND NOT c:SPEC
                AND c.token < 1000
              WITH c, labels(c) AS cLabels, coalesce(toInteger(c.startLine), 0) AS sortKey
              RETURN c AS n, cLabels AS nodeLabels, NULL AS r, NULL AS m, sortKey
              
              UNION ALL
              
              // token >= 1000인 큰 노드 → 작은 노드를 만날 때까지 재귀 탐색
              WITH p
              MATCH (p)-[:PARENT_OF]->(c)
              WHERE NOT c:DECLARE AND NOT c:Table AND NOT c:SPEC
                AND coalesce(toInteger(c.token), 0) >= 1000
              // 큰 노드부터 자손 탐색
              WITH c
              MATCH path = (c)-[:PARENT_OF*0..]->(n)
              WHERE NOT n:DECLARE AND NOT n:Table AND NOT n:SPEC
              // 경로상 모든 노드의 token 체크
              WITH n, path, nodes(path) AS pathNodes
              // 핵심: 경로의 모든 부모가 큰 노드(token >= 1000)이거나, 
              //       n이 첫 번째 작은 노드(token < 1000)인 경우만 반환
              WHERE ALL(i IN range(0, size(pathNodes)-2) 
                        WHERE coalesce(toInteger(pathNodes[i].token), 0) >= 1000)
              OPTIONAL MATCH (n)-[r]->(m {{
                folder_name: '{folder_name}', file_name: '{file_name}', user_id: '{user_id}'
              }})
              WHERE r IS NULL
                 OR ( NOT (m:DECLARE OR m:Table OR m:SPEC)
                      AND none(x IN ['CALL','WRITES','FROM'] WHERE type(r) CONTAINS x) )
              WITH n, labels(n) AS nLabels, r, m, coalesce(toInteger(n.startLine), 0) AS sortKey
              RETURN DISTINCT n, nLabels AS nodeLabels, r, m, sortKey
            }}
            
            RETURN n, nodeLabels, r, m
            ORDER BY sortKey, coalesce(toInteger(n.token), 0), id(n)
            """,
            f"""
            MATCH (n {{folder_name: '{folder_name}', file_name: '{file_name}', 
                     procedure_name: '{procedure_name}', user_id: '{user_id}'}})
            WHERE n:DECLARE
            MATCH (n)-[:SCOPE]->(v:Variable)
            RETURN v
            """
        ])

        # 전처리 수행
        generator = ServicePreprocessingGenerator(
            service_nodes,
            variable_nodes,
            command_class_variable,
            service_skeleton,
            query_method_list,
            folder_name,
            file_name,
            procedure_name,
            sequence_methods,
            user_id,
            api_key,
            locale,
            project_name
        )

        await generator.generate()
        
        # 🚀 성능 최적화된 자동 파일 저장
        service_class_name = convert_to_pascal_case(procedure_name) + "Service"
        await generator._save_service_file(service_class_name)

        logging.info("\n" + "-"*80)
        logging.info(f"✅ STEP 4 완료: {service_class_name} 생성 및 저장 완료")
        logging.info("-"*80 + "\n")

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"서비스 전처리 중 오류: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
    finally:
        await connection.close()
