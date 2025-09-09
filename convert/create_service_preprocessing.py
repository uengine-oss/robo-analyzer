import logging
import textwrap

from prompt.convert_service_prompt import convert_service_code
from prompt.convert_summarized_service_skeleton_prompt import convert_summarized_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import extract_used_query_methods, collect_variables_in_range



class ServicePreprocessor:
    """
    역할:
      - 서비스 전처리 전체 라이프사이클 관리
      - 단일 컨텍스트 누적(메모리) 방식으로 자바 코드 생성 흐름 구성
      - 대용량 부모(토큰≥1500, 자식 보유) 스켈레톤 관리 및 자식 코드/스켈레톤 단일 치환 처리
      - 토큰 임계(기본 1500) 도달 시 LLM 분석 수행(변수/JPA 추출), DB 업데이트는 하지 않음

    매개변수:
      - traverse_nodes(list[dict]): 그래프에서 조회한 비즈니스 노드 레코드들({'n','r','m','nType'} 등)
      - variable_nodes(list[dict]): 변수 범위 정보를 담은 노드 리스트({'v': Variable})
      - connection(Neo4jConnection): Neo4j 연결 객체
      - command_class_variable(dict): 커맨드 클래스 필드 정의 정보
      - service_skeleton(str): 서비스 메서드 스켈레톤 템플릿
      - query_method_list(list|dict): 사용 가능한 JPA 쿼리 메서드 목록
      - object_name(str): 오브젝트(패키지)명
      - procedure_name(str): 프로시저명
      - sequence_methods(list): 시퀀스 메서드 목록
      - user_id(str): 사용자 ID
      - api_key(str): LLM API 키
      - locale(str): 로케일
    """

    TOKEN_THRESHOLD = 1500
    CODE_PLACEHOLDER = "...code..."

    def __init__(self, traverse_nodes: list, variable_nodes: list, connection: Neo4jConnection,
                 command_class_variable: dict, service_skeleton: str, query_method_list: dict,
                 object_name: str, procedure_name: str, sequence_methods: list, user_id: str,
                 api_key: str, locale: str) -> None:
        self.traverse_nodes = traverse_nodes
        self.variable_nodes = variable_nodes
        self.connection = connection
        self.command_class_variable = command_class_variable
        self.service_skeleton = service_skeleton
        self.query_method_list = query_method_list
        self.object_name = object_name
        self.procedure_name = procedure_name
        self.sequence_methods = sequence_methods
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale

        # 상태 값
        self.merged_java_code = ""  
        self.total_tokens = 0
        self.used_variables = []
        self.used_query_method_dict = {}
        self.tracking_variables = {}
        self.current_parent = None 
        self.java_buffer = ""
        self.sp_code = ""
        self.sp_range = {"startLine": None, "endLine": None}

    #==================================================================
    # 로깅/출력
    #==================================================================
    def _log_node_info(self, record: dict) -> None:
        """
        역할:
          - 노드의 기본 정보를 로그로 남김(가독성 향상)

        매개변수:
          - record(dict): 순회 중인 레코드(키 'n','r','m','nType' 등 포함 가능)
        """
        start_node = record['n']
        n_type = record.get('nType') or ''
        token = int(start_node.get('token', 0) or 0)
        start_line = int(start_node.get('startLine', 0) or 0)
        end_line = int(start_node.get('endLine', 0) or 0)
        rel = record.get('r')
        relationship = rel[1] if rel else 'NEXT'
        logging.info(f"[Node] type={n_type} start={start_line} end={end_line} token={token} rel={relationship}")

    #==================================================================
    # 대용량 스켈레톤 처리
    #==================================================================
    async def _generate_large_node_code(self, summarized_code: str) -> str:
        """
        역할:
          - 요약된 자식 코드가 포함된 큰 노드의 요약 코드를 분석하여 자바 스켈레톤을 생성

        매개변수:
          - summarized_code(str): 자식이 "...code..." 등으로 요약된 코드 문자열

        반환값:
          - str: 생성된 자바 스켈레톤 코드
        """
        analysis_result = convert_summarized_code(summarized_code, self.api_key, self.locale)
        return analysis_result['code']

    def _insert_into_parent(self, child_start: int, child_code: str) -> bool:
        """
        역할:
          - 현재 부모 스켈레톤의 일반 플레이스홀더("...code...")를 1회 치환

        매개변수:
          - child_start(int): 사용되지 않음(호출 시 0 전달), 인터페이스 호환용
          - child_code(str): 부모 내부에 삽입할 자식(또는 누적된) 코드 문자열

        반환값:
          - bool: 치환 성공 여부
        """
        if not self.current_parent:
            return False
        placeholder = self.CODE_PLACEHOLDER
        self.current_parent['code'] = self.current_parent['code'].replace(
            placeholder, f"\n{textwrap.indent(child_code, '    ')}", 1
        )
        return True

    async def _finalize_parent_if_passed(self, current_start_line: int, relationship: str) -> None:
        """
        역할:
          - 현재 노드가 부모 범위를 벗어났는지 판단하고, 벗어났다면 부모를 마무리

        매개변수:
          - current_start_line(int): 현재 노드의 시작 라인
          - relationship(str): 현재 레코드의 관계 타입(주로 'NEXT')
        """
        if not self.current_parent:
            return
        if relationship == 'NEXT' and current_start_line > self.current_parent['end']:
            # 부모 종료 전에 남은 sp_code가 있으면 분석해서 java_buffer에 반영
            if self.sp_code:
                await self._analyze_and_update()
            await self._finalize_current_parent()

    async def _finalize_current_parent(self) -> None:
        """
        역할:
          - 현재 부모의 "...code..."에 누적된 자식 코드(java_buffer)를 1회 치환하고,
            완성된 부모 코드를 최종 컨텍스트에 병합
        """
        if not self.current_parent:
            return
        if self.java_buffer:
            self._insert_into_parent(0, self.java_buffer.strip('\n'))
        self.merged_java_code += f"\n{self.current_parent['code']}"
        self.total_tokens += self.TOKEN_THRESHOLD
        self.current_parent = None
        self.java_buffer = ""

    # (컨텍스트 범위 관리는 pending_range로만 수행)

    #==================================================================
    # 대용량 노드/일반 노드 처리
    #==================================================================
    async def _handle_large_node(self, summarized_code: str, start_line: int, end_line: int, token: int) -> None:
        """
        역할:
          - 큰 노드(자식 있음, 토큰 임계 이상)를 처리하여 스켈레톤을 적용

        매개변수:
          - summarized_code(str): 요약 코드
          - start_line(int): 노드 시작 라인
          - end_line(int): 노드 끝 라인
          - token(int): 노드 토큰 수
        """
        skeleton = await self._generate_large_node_code(summarized_code)
        # 루트 부모가 없으면 현재 노드를 부모로 설정, 있으면 즉시 부모에 치환
        if not self.current_parent:
            self.current_parent = {'start': start_line, 'end': end_line, 'code': skeleton}
        else:
            self._insert_into_parent(0, skeleton)
        self.total_tokens += token

    def _handle_small_or_leaf_node(self, node_code: str, token: int, start_line: int, end_line: int) -> None:
        """
        역할:
          - 작은 노드 또는 자식 없는 큰 노드를 처리(부모 진행 중이면 버퍼, 아니면 즉시 누적)

        매개변수:
          - node_code(str): 자바 코드 조각
          - token(int): 토큰 수
        """
        # 작은/자식없음 노드는 원본을 sp_code에 누적하여 임계 시 LLM 변환
        self.sp_code += f"\n{node_code}"
        self.total_tokens += token
        if self.sp_range['startLine'] is None or start_line < self.sp_range['startLine']:
            self.sp_range['startLine'] = start_line
        if self.sp_range['endLine'] is None or end_line > self.sp_range['endLine']:
            self.sp_range['endLine'] = end_line

    #==================================================================
    # 분석 및 변수/JPA 업데이트
    #==================================================================
    async def _maybe_analyze(self) -> None:
        """
        역할:
          - 토큰 임계 도달 시 LLM 분석 수행(변수/JPA 수집 후 분석 실행)
        """
        if self.total_tokens >= self.TOKEN_THRESHOLD:
            await self._analyze_and_update()

    #==================================================================
    # 분석/업데이트
    #==================================================================
    async def _update_variables(self, analysis_result: dict) -> None:
        """LLM 분석 결과의 변수 추적 정보를 메모리에만 반영합니다(DB 미반영)."""
        variables_info = analysis_result['analysis'].get('variables', {})
        for var_name, var_info in variables_info.items():
            self.tracking_variables[var_name] = var_info

    async def _analyze_and_update(self) -> None:
        """
        역할:
          - 현재 누적 컨텍스트로 LLM 분석을 수행하고, 변수/JPA 수집 정보를 기반으로
            변수 추적 상태만 메모리에 반영
        """
        if not self.sp_code or self.sp_range['startLine'] is None or self.sp_range['endLine'] is None:
            return
        start_line_ctx = self.sp_range['startLine']
        end_line_ctx = self.sp_range['endLine']
        context_range = [{"startLine": start_line_ctx, "endLine": end_line_ctx}]

        try:
            collected = await collect_variables_in_range(self.variable_nodes, start_line_ctx, end_line_ctx)
            self.used_variables = [
                {**v, 'role': self.tracking_variables.get(v['name'], '')}
                for v in collected
            ]
        except Exception as _e:
            logging.debug(f"변수 수집 스킵: {_e}")

        try:
            self.used_query_method_dict = await extract_used_query_methods(
                start_line_ctx, end_line_ctx, self.query_method_list, {}
            )
        except Exception as _e:
            logging.debug(f"JPA 수집 스킵: {_e}")

        analysis_result = convert_service_code(
            self.sp_code,
            self.service_skeleton,
            self.used_variables,
            self.command_class_variable,
            context_range,
            1,
            self.used_query_method_dict,
            self.sequence_methods,
            self.api_key,
            self.locale
        )
        await self._update_variables(analysis_result)
        # 생성된 자바 코드를 누적 (부모 진행 중이면 java_buffer, 아니면 merged_java_code)
        code_map = analysis_result.get('analysis', {}).get('code', {}) or {}
        def _key_order(k: str) -> int:
            parts = str(k).replace('-', '~').split('~')
            try:
                return int(parts[0])
            except Exception:
                return 0
        generated_java = "\n".join(code_map[k] for k in sorted(code_map.keys(), key=_key_order))
        if generated_java:
            if self.current_parent:
                self.java_buffer += f"\n{generated_java}"
                else:
                self.merged_java_code += f"\n{generated_java}"

        # 임계 초기화
        self.total_tokens = 0
        self.used_variables.clear()
        self.used_query_method_dict.clear()
        self.sp_code = ""
        self.sp_range = {"startLine": None, "endLine": None}

    #==================================================================
    # 메인 처리
    #==================================================================
    async def process(self) -> None:
        """
        역할:
          - 전체 노드를 순회하며 단일 컨텍스트 누적과 대용량 스켈레톤 병합, 임계 분석 트리거를 수행
        """
        for record in self.traverse_nodes:
            start_node = record['n']
            has_children = bool(start_node.get('has_children', False))
            token = int(start_node.get('token', 0) or 0)
            start_line = int(start_node.get('startLine', 0) or 0)
            end_line = int(start_node.get('endLine', 0) or 0)
            rel = record.get('r')
            relationship = rel[1] if rel else 'NEXT'

            # 노드 정보 출력
            self._log_node_info(record)

            # 부모 종료 판단 및 마무리
            await self._finalize_parent_if_passed(start_line, relationship)

            # 범위 갱신은 pending_code 누적 시점에서만 수행

            # 분기: 큰 부모 vs 일반 노드
            if token >= self.TOKEN_THRESHOLD and has_children:
                await self._handle_large_node(start_node.get('summarized_code', '') or '', start_line, end_line, token)
            else:
                # 작은/자식없음 노드 처리
                self._handle_small_or_leaf_node(start_node.get('node_code', ''), token, start_line, end_line)

            await self._maybe_analyze()

        # 남아 있는 부모 정리(1회 치환 후 병합)
        if self.current_parent:
            # 부모 마무리 전에 남은 pending 변환을 먼저 처리
            if self.pending_code:
                await self._analyze_and_update()
            await self._finalize_current_parent()

        # 남은 변환 대기 코드가 있으면 마지막 분석 실행
        if self.pending_code:
            await self._analyze_and_update()


async def start_service_preprocessing(service_skeleton: str, command_class_variable: dict, procedure_name: str,
                                      query_method_list: dict, object_name: str, sequence_methods: list, user_id: str,
                                      api_key: str, locale: str) -> tuple:
    """
    역할:
      - 서비스 코드 생성을 시작합니다.

    매개변수:
      - service_skeleton(str): 서비스 메서드 스켈레톤 템플릿
      - command_class_variable(dict): 커맨드 클래스 필드 정의 정보
      - procedure_name(str): 프로시저 이름
      - query_method_list(dict): 사용 가능한 쿼리 메서드 목록
      - object_name(str): 패키지/프로시저 이름
      - sequence_methods(list): 시퀀스 메서드 목록
      - user_id(str): 사용자 ID
      - api_key(str): LLM API 키
      - locale(str): 로케일

    반환값:
      - (variable_nodes, merged_java_code): 변수 노드 리스트와 최종 병합된 자바 코드
    """
    
    connection = Neo4jConnection() 
    logging.info(f"[{object_name}] {procedure_name} 프로시저의 서비스 코드 생성을 시작합니다.")
    
    try:
        node_query = [
            f"""
            MATCH (p) 
            WHERE p.object_name = $object_name 
                AND p.procedure_name = $procedure_name 
                AND p.user_id = $user_id 
                AND (p:FUNCTION OR p:PROCEDURE OR p:CREATE_PROCEDURE_BODY OR p:TRIGGER) 
            MATCH (p)-[:PARENT_OF]->(c) 
            WHERE NOT (c:ROOT OR c:Variable OR c:DECLARE OR c:Table OR c:SPEC) 
            MATCH path = (c)-[:PARENT_OF*0..]->(n) 
            WHERE NOT (n:ROOT OR n:Variable OR n:DECLARE OR n:Table OR n:SPEC)
            OPTIONAL MATCH (n)-[r]->(m) 
            WHERE m.object_name = $object_name 
                AND m.user_id = $user_id 
                AND NOT (m:ROOT OR m:Variable OR m:DECLARE OR m:Table OR m:SPEC) 
                AND NOT type(r) CONTAINS 'CALL' 
                AND NOT type(r) CONTAINS 'WRITES' 
                AND NOT type(r) CONTAINS 'FROM' 
            RETURN DISTINCT n, r, m ORDER BY n.startLine
            """,
            f"""
            MATCH (n)
            WHERE n.object_name = '{object_name}'
            AND n.procedure_name = '{procedure_name}'
            AND n.user_id = '{user_id}'
            AND (n:DECLARE)
            MATCH (n)-[r:SCOPE]->(v:Variable)
            RETURN v
            """
        ]

        service_nodes, variable_nodes = await connection.execute_queries(node_query)        

        processor = ServicePreprocessor(
            service_nodes, 
            variable_nodes,
            connection, 
            command_class_variable, 
            service_skeleton, 
            query_method_list, 
            object_name, 
            procedure_name,
            sequence_methods,
            user_id,
            api_key,
            locale
        )
        await processor.process()

        final_code = processor.merged_java_code.strip()
        logging.info(f"[{object_name}] {procedure_name} 프로시저의 서비스 코드 생성이 완료되었습니다.\n")
        return variable_nodes, final_code
    except ConvertingError: 
        raise
    except Exception as e:
        err_msg = f"(전처리) 서비스 코드 생성 준비 중 오류: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
    finally:
        await connection.close()