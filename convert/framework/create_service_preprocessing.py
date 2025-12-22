import logging
import textwrap
import json
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import extract_used_query_methods, collect_variables_in_range, build_rule_based_path, save_file, convert_to_pascal_case
from util.rule_loader import RuleLoader


# ----- 상수 정의 -----
TOKEN_THRESHOLD = 1000
CODE_PLACEHOLDER = "...code..."


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
        'query_method_list', 'directory', 'file_name', 'procedure_name', 'sequence_methods',
        'user_id', 'api_key', 'locale', 'project_name', 'target_lang',
        'merged_chunks', 'total_tokens', 'tracking_variables', 'parent_stack',
        'sp_code_parts', 'sp_start', 'sp_end', 'pending_try_mode', 'try_buffer',
        'rule_loader'
    )

    def __init__(self, traverse_nodes: list, variable_nodes: list, command_class_variable: dict,
                 service_skeleton: str, query_method_list: dict, directory: str, file_name: str,
                 procedure_name: str, sequence_methods: list, user_id: str, api_key: str, locale: str, 
                 project_name: str = "demo", target_lang: str = 'java'):
        self.traverse_nodes = traverse_nodes
        self.variable_nodes = variable_nodes
        self.command_class_variable = command_class_variable
        self.service_skeleton = service_skeleton
        self.query_method_list = query_method_list
        self.directory = directory
        self.file_name = file_name
        self.procedure_name = procedure_name
        self.sequence_methods = sequence_methods
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name or "demo"
        self.target_lang = target_lang

        # 상태 초기화
        self.merged_chunks: list[str] = []
        self.total_tokens = 0
        self.tracking_variables: dict = {}
        self.parent_stack: list[dict] = []
        self.sp_code_parts: list[str] = []
        self.sp_start: int | None = None
        self.sp_end: int | None = None

        # TRY-EXCEPTION 처리
        self.pending_try_mode = False
        self.try_buffer: list[str] = []

        # Rule 파일 로더
        self.rule_loader = RuleLoader(target_lang=target_lang)

    # ----- 공개 메서드 -----

    @staticmethod
    def _resolve_node_type(node_labels: list | None, node: dict) -> str:
        raw_type = node_labels[0] if node_labels else node.get('name', 'UNKNOWN')
        raw_type = str(raw_type)
        return raw_type.split('[')[0] if '[' in raw_type else raw_type

    @staticmethod
    def _safe_int(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    async def generate(self) -> str:
        """
        전체 노드를 순회하며 자바 코드 생성
        
        Returns:
            str: 최종 병합된 자바 코드
        """
        logging.info("📋 노드 순회 시작")

        seen_nodes = set()
        node_count = 0
        for record in self.traverse_nodes:
            node = record['n']
            start_line = self._safe_int(node.get('startLine'))
            end_line = self._safe_int(node.get('endLine'))
            node_key = (start_line, end_line)
            if node_key in seen_nodes:
                continue
            seen_nodes.add(node_key)
            node_count += 1
            await self._process_node(record)

        await self._finalize_remaining()

        logging.info(f"✅ 총 {node_count}개 노드 처리 완료\n")
        return self._final_output()

    # ----- 노드 처리 -----

    async def _process_node(self, record: dict) -> None:
        """단일 노드 처리"""
        node = record['n']
        node_labels = record.get('nodeLabels', [])
        node_type = self._resolve_node_type(node_labels, node)
        has_children = bool(node.get('has_children', False))
        token = self._safe_int(node.get('token'))
        start_line = self._safe_int(node.get('startLine'))
        end_line = self._safe_int(node.get('endLine'))
        logging.debug(
            "→ %s[%s~%s] 토큰=%s | 자식=%s",
            node_type,
            start_line,
            end_line,
            token,
            "있음" if has_children else "없음"
        )

        if node_type == 'TRY':
            self.pending_try_mode = True
            logging.info("  🔒 TRY 노드 감지 → EXCEPTION까지 merge 보류")

        if node_type == 'EXCEPTION':
            await self._handle_exception_node(node, start_line, end_line)
            return

        while self.parent_stack and start_line > self.parent_stack[-1]['end']:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            await self._finalize_parent()

        is_large_parent = token >= TOKEN_THRESHOLD and has_children
        is_large_leaf = token >= TOKEN_THRESHOLD and not has_children

        if is_large_parent:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            logging.info("  ┌─ 큰 노드 진입 [%s~%s] (토큰: %s)", start_line, end_line, token)
            await self._handle_large_node(node, start_line, end_line, token)
        else:
            if is_large_leaf:
                if self.sp_code_parts:
                    await self._analyze_and_merge()
            else:
                await self._flush_pending_accumulation(token)
            self._handle_small_node(node, start_line, end_line, token)

        if is_large_leaf:
            logging.info("  ⚠️  단독 대용량 리프 노드 변환 실행")
            await self._analyze_and_merge()
        elif self.total_tokens >= TOKEN_THRESHOLD:
            logging.info("  ⚠️  토큰 임계값 도달 (%s) → LLM 분석 실행", self.total_tokens)
            await self._analyze_and_merge()

    # ----- 대용량 노드 처리 -----

    async def _handle_large_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        """대용량 노드(자식 있음, 토큰≥1000) 처리"""
        summarized = (node.get('summarized_code') or '').strip()
        if not summarized:
            return
        
        # 현재 컨텍스트 수집
        used_vars, used_queries = await self._collect_current_context()

        # LLM으로 스켈레톤 생성 (Rule 파일 사용)
        result = self.rule_loader.execute(
            role_name='service_summarized',
            inputs={
                'summarized_code': summarized,
                'service_skeleton': json.dumps(self.service_skeleton, ensure_ascii=False),
                'variable': json.dumps(used_vars, ensure_ascii=False, indent=2),
                'command_variables': json.dumps(self.command_class_variable, ensure_ascii=False, indent=2),
                'query_method_list': json.dumps(used_queries, ensure_ascii=False, indent=2),
                'sequence_methods': json.dumps(self.sequence_methods, ensure_ascii=False, indent=2),
                'locale': self.locale
            },
            api_key=self.api_key
        )
        skeleton = result['code']

        entry = {
            'start': start_line,
            'end': end_line,
            'code': skeleton,
            'children': []
        }
        self.parent_stack.append(entry)
        logging.info("  │  부모 push 완료 (stack=%s)", len(self.parent_stack))


    # ----- 소형 노드 처리 -----

    def _handle_small_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        """소형 노드 또는 리프 노드 처리"""
        node_code = (node.get('node_code') or '').strip()
        if not node_code:
            return

        # SP 코드 누적
        self.sp_code_parts.append(node_code)
        self.total_tokens += int(token or 0)

        # 범위 업데이트
        if self.sp_start is None or start_line < self.sp_start:
            self.sp_start = start_line
        if self.sp_end is None or end_line > self.sp_end:
            self.sp_end = end_line

    async def _flush_pending_accumulation(self, incoming_token: int | None) -> None:
        """다음 노드 추가 전에 임계값 초과 여부 확인"""
        if (
            self.sp_code_parts
            and incoming_token is not None
            and (self.total_tokens + int(incoming_token or 0)) >= TOKEN_THRESHOLD
        ):
            logging.info("  ⚠️  다음 노드 추가 시 토큰 초과 예상 → 기존 누적 변환")
            await self._analyze_and_merge()

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
        if not self.parent_stack:
            return

        entry = self.parent_stack.pop()
        logging.info(
            "  └─ 큰 노드 완료 [%s~%s] (stack→%s)",
            entry['start'],
            entry['end'],
            len(self.parent_stack)
        )

        code = self._merge_regular_children(entry['code'], entry.get('children', []))
        code = code.strip()
        self._add_child_code(code, entry.get('start'), entry.get('end'))

    def _merge_regular_children(self, code: str, children: list) -> str:
        """부모 placeholder에 자식 코드 삽입"""
        child_block = "\n".join(
            child for child in children or [] if isinstance(child, str) and child.strip()
        ).strip()

        if CODE_PLACEHOLDER in code:
            if child_block:
                indented = textwrap.indent(child_block, '    ')
                return code.replace(CODE_PLACEHOLDER, f"\n{indented}\n", 1)
            return code.replace(CODE_PLACEHOLDER, "", 1)

        if not child_block:
            return code

        indented = textwrap.indent(child_block, '    ')
        return f"{code}\n{indented}"

    def _add_child_code(self, code: str, start_line: int | None = None, end_line: int | None = None) -> None:
        """생성된 코드를 부모 또는 최종 코드에 추가"""
        if not code or not code.strip():
            return

        if self.parent_stack:
            parent_entry = self.parent_stack[-1]
            parent_entry.setdefault('children', []).append(code.strip())
            logging.info(
                "      ➕ 부모 children 추가 | 부모라인=%s~%s | child_count=%s",
                parent_entry.get('start'),
                parent_entry.get('end'),
                len(parent_entry['children'])
            )
            return

        target = self.try_buffer if self.pending_try_mode else self.merged_chunks
        target.append(code.strip())
        logging.info("      ➕ %s에 변환 결과 추가", "TRY 버퍼" if self.pending_try_mode else "최종 코드")

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
        logging.info("  ⚡ EXCEPTION 노드 감지 → 예외처리 구조 생성 시작")

        if self.sp_code_parts:
            await self._analyze_and_merge()

        node_code = (node.get('node_code') or '').strip()
        if not node_code:
            logging.warning("     ⚠️  EXCEPTION 노드 코드가 비어있음")
            return

        result = self.rule_loader.execute(
            role_name='service_exception',
            inputs={
                'node_code': node_code,
                'locale': self.locale
            },
            api_key=self.api_key
        )
        exception_java_code = result.get('code', '').strip()

        if 'CodePlaceHolder' not in exception_java_code:
            logging.warning("     ⚠️  try-catch 템플릿에 CodePlaceHolder가 없음")
            return

        if self.pending_try_mode:
            try_block_code = "\n".join(self.try_buffer).strip()
            wrapped_code = exception_java_code.replace('CodePlaceHolder', try_block_code)
            if wrapped_code.strip():
                self.merged_chunks.append(wrapped_code)
            logging.info("     ✓ TRY 블록 코드를 예외처리로 감쌈")
        else:
            entire_code = self._final_output()
            wrapped_code = exception_java_code.replace('CodePlaceHolder', entire_code)
            self.merged_chunks = [wrapped_code]
            logging.info("     ✓ 전체 메서드 코드를 예외처리로 감쌈")

        self.try_buffer.clear()
        self.pending_try_mode = False
        logging.info("     ✓ 예외처리 완료 및 상태 초기화")

    # ----- 분석 및 병합 -----

    async def _analyze_and_merge(self) -> None:
        """LLM 분석 및 자바 코드 병합"""
        if not self.sp_code_parts or self.sp_start is None:
            return

        # 문자열 조인
        sp_code = '\n'.join(self.sp_code_parts)
        if self.parent_stack:
            target = "부모 children"
        elif self.pending_try_mode:
            target = "TRY 버퍼"
        else:
            target = "최종코드"
        logging.info(
            "  🤖 LLM 분석 시작: [%s~%s] %s개 파트 (토큰: %s) → %s",
            self.sp_start,
            self.sp_end,
            len(self.sp_code_parts),
            self.total_tokens,
            target
        )

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

        # LLM 분석 (Role 파일 사용)
        result = self.rule_loader.execute(
            role_name='service',
            inputs={
                'code': sp_code,
                'service_skeleton': json.dumps(self.service_skeleton, ensure_ascii=False),
                'variable': json.dumps(used_variables, ensure_ascii=False, indent=2),
                'query_method_list': json.dumps(used_query_methods, ensure_ascii=False, indent=2),
                'sequence_methods': json.dumps(self.sequence_methods, ensure_ascii=False, indent=2),
                'locale': self.locale,
                'parent_code': self.parent_stack[-1]['code'] if self.parent_stack else ""
            },
            api_key=self.api_key
        )

        analysis = result.get('analysis', {}) or {}
        self.tracking_variables.update(analysis.get('variables', {}))

        java_code = (analysis.get('code') or '').strip()
        if java_code:
            self._add_child_code(java_code, self.sp_start, self.sp_end)

        # 상태 초기화
        self.total_tokens = 0
        self.sp_code_parts.clear()
        self.sp_start = None
        self.sp_end = None

    # ----- 마무리 -----

    async def _finalize_remaining(self) -> None:
        """남은 데이터 정리"""
        if self.parent_stack:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            while self.parent_stack:
                await self._finalize_parent()
        elif self.sp_code_parts:
            await self._analyze_and_merge()

    def _final_output(self) -> str:
        """누적된 자바 코드를 단일 문자열로 반환"""
        chunks = list(self.merged_chunks)
        if self.pending_try_mode and self.try_buffer:
            chunks.extend(self.try_buffer)
        return "\n".join(chunk for chunk in chunks if chunk and chunk.strip()).strip()

    async def _save_service_file(self, service_class_name: str) -> str:
        """성능 최적화된 서비스 파일 자동 저장"""
        try:
            # 병합된 Java 코드를 서비스 스켈레톤에 삽입
            completed_service_code = self.service_skeleton.replace("CodePlaceHolder", self._final_output())
            
            # 저장 경로 설정 (Rule 파일 기반)
            base_path = build_rule_based_path(self.project_name, self.user_id, self.rule_loader.target_lang, 'service')
            
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
    directory: str,
    file_name: str,
    sequence_methods: list,
    project_name: str,
    user_id: str,
    api_key: str,
    locale: str,
    target_lang: str = 'java'
) -> tuple:
    """
    서비스 전처리 시작
    
    Args:
        service_skeleton: 서비스 메서드 스켈레톤 템플릿
        command_class_variable: 커맨드 클래스 필드 정의
        procedure_name: 프로시저 이름
        query_method_list: JPA 쿼리 메서드 목록
        directory: 디렉토리 경로
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
    logging.info(f"📁 파일: {directory}/{file_name}")

    # Neo4j 쿼리용 정규화된 directory (Windows 경로 구분자 통일)
    directory_normalized = directory.replace('\\', '/') if directory else ''

    try:
        # Neo4j 쿼리
        service_nodes, variable_nodes = await connection.execute_queries([
            f"""
            MATCH (p:PROCEDURE {{
              directory: '{directory_normalized}',
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
                directory: '{directory_normalized}', file_name: '{file_name}', user_id: '{user_id}'
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
            MATCH (n {{directory: '{directory_normalized}', file_name: '{file_name}', 
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
            directory,
            file_name,
            procedure_name,
            sequence_methods,
            user_id,
            api_key,
            locale,
            project_name,
            target_lang
        )

        await generator.generate()
        
        # 🚀 성능 최적화된 자동 파일 저장
        service_class_name = convert_to_pascal_case(procedure_name) + "Service"
        service_code = await generator._save_service_file(service_class_name)

        logging.info("\n" + "-"*80)
        logging.info(f"✅ STEP 4 완료: {service_class_name} 생성 및 저장 완료")
        logging.info("-"*80 + "\n")
        
        return service_code

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"서비스 전처리 중 오류: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
    finally:
        await connection.close()
