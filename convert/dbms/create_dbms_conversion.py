import logging
import textwrap
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import (
    build_rule_based_path, save_file,
    build_converting_root_query, build_conversion_block_query
)
from util.rule_loader import RuleLoader
from convert.dbms.create_dbms_skeleton import start_dbms_skeleton


# ----- 상수 정의 -----
TOKEN_THRESHOLD = 1000
CODE_PLACEHOLDER = "...code..."
DML_TYPES = frozenset(["SELECT", "INSERT", "UPDATE", "DELETE", "FETCH", "MERGE", "JOIN", "ALL_UNION", "UNION"])


# ----- DBMS 변환 클래스 -----
class DbmsConversionGenerator:
    """
    DBMS 변환 전체 라이프사이클 관리
    - 단일 컨텍스트 누적 방식으로 타겟 DBMS 코드 생성
    - 대용량 부모(토큰≥1000, 자식 보유) 스켈레톤 관리
    - 토큰 임계 도달 시 LLM 분석 수행
    """
    __slots__ = (
        'traverse_nodes', 'folder_name', 'file_name', 'procedure_name',
        'user_id', 'api_key', 'locale', 'project_name', 'target_dbms', 'skeleton_code',
        'merged_code', 'total_tokens', 'parent_stack', 'top_level_begin_skipped',
        'sp_code_parts', 'sp_start', 'sp_end',
        'rule_loader', 'conversion_queries', 'last_block_range'
    )

    def __init__(self, traverse_nodes: list, folder_name: str, file_name: str,
                 procedure_name: str, user_id: str, api_key: str, locale: str, 
                 project_name: str = "demo", target_dbms: str = "oracle",
                 skeleton_code: str | None = None):
        self.traverse_nodes = traverse_nodes
        self.folder_name = folder_name
        self.file_name = file_name
        self.procedure_name = procedure_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name or "demo"
        self.target_dbms = target_dbms
        self.skeleton_code = (skeleton_code or "").strip()

        # 상태 초기화
        self.merged_code = ""
        self.total_tokens = int(0)
        self.parent_stack = []
        self.top_level_begin_skipped = False
        self.sp_code_parts = []
        self.sp_start = None
        self.sp_end = None
        self.conversion_queries = []
        self.last_block_range = None  # (start_line, end_line) - NEXT 관계용
        
        # Rule 파일 로더 (target_dbms로 디렉토리 찾음)
        self.rule_loader = RuleLoader(target_lang=target_dbms)

    # ----- 공개 메서드 -----

    async def generate(self) -> str:
        """
        전체 노드를 순회하며 타겟 DBMS 코드 생성
        
        Returns:
            str: 최종 병합된 코드
        """
        logging.info(f"📋 DBMS 변환 노드 순회 시작: postgres → {self.target_dbms}")

        # CONVERTING 루트 노드 생성 (변환 시작 시 한 번만)
        root_query = build_converting_root_query(
            folder_name=self.folder_name,
            file_name=self.file_name,
            procedure_name=self.procedure_name,
            user_id=self.user_id,
            project_name=self.project_name,
            conversion_type="dbms",
            target=self.target_dbms
        )
        self.conversion_queries.append(root_query)

        # 중복 제거: 같은 라인 범위는 한 번만 처리
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
        return self.merged_code.strip()

    # ----- 노드 처리 -----

    async def _process_node(self, record: dict) -> None:
        """단일 노드 처리"""
        node = record['n']
        node_labels = record.get('nodeLabels', [])
        node_type = node_labels[0] if node_labels else node.get('name', 'UNKNOWN')
        has_children = bool(node.get('has_children', False))
        token = int(node.get('token', 0) or 0)
        start_line = int(node.get('startLine', 0) or 0)
        end_line = int(node.get('endLine', 0) or 0)
        relationship = record['r'][1] if record.get('r') else 'NEXT'

        # 노드 처리 로그
        readable_type = node_type.split('[')[0] if '[' in str(node_type) else str(node_type)
        logging.info(
            "➡️  노드 감지 | 타입=%s | 라인=%s~%s | 토큰=%s | 관계=%s | 자식=%s | stack_depth=%s",
            readable_type,
            start_line,
            end_line,
            token,
            relationship,
            "있음" if has_children else "없음",
            len(self.parent_stack)
        )

        # 부모 경계 체크
        while self.parent_stack and start_line > self.parent_stack[-1]['end']:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            await self._finalize_parent()

        # 최상위 BEGIN 블록은 스켈레톤이 처리하므로 스킵
        if (readable_type == "BEGIN"
                and not self.top_level_begin_skipped
                and not self.parent_stack
                and not self.merged_code):
            self.top_level_begin_skipped = True
            logging.info("    ⛔ 최상위 BEGIN 블록 스킵 (스켈레톤에서 처리)")
            return

        # 노드 타입별 처리
        is_large_parent = token >= TOKEN_THRESHOLD and has_children and node_type not in DML_TYPES
        is_large_leaf = token >= TOKEN_THRESHOLD and not has_children

        if is_large_parent:
            # 큰 노드 처리 전에 쌓인 작은 노드들 먼저 변환
            if self.sp_code_parts:
                await self._analyze_and_merge()
            
            logging.info(
                "    🧱 대용량 노드 처리 준비 | 라인=%s~%s | 토큰=%s | 현재 stack=%s",
                start_line,
                end_line,
                token,
                len(self.parent_stack)
            )
            await self._handle_large_node(node, start_line, end_line, token)
        else:
            if is_large_leaf:
                if self.sp_code_parts:
                    await self._analyze_and_merge()
            else:
                await self._flush_pending_accumulation(token)

            logging.info(
                "    ✏️ 일반 노드 누적 | 라인=%s~%s | 토큰=%s | 현재 stack=%s",
                start_line,
                end_line,
                token,
                len(self.parent_stack)
            )
            self._handle_small_node(node, start_line, end_line, token)

        # 임계값 체크
        if is_large_leaf:
            logging.info("    ⚠️  단독 대용량 리프 노드 변환 실행")
            await self._analyze_and_merge()
        elif int(self.total_tokens) >= TOKEN_THRESHOLD:
            logging.info("    ⚠️  토큰 누적 %s ≥ %s → LLM 분석 실행", int(self.total_tokens), TOKEN_THRESHOLD)
            await self._analyze_and_merge()

    # ----- 대용량 노드 처리 -----

    async def _handle_large_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        """대용량 노드(자식 있음, 토큰≥1000) 처리"""
        summarized = (node.get('summarized_code') or '').strip()
        if not summarized:
            logging.info("      ⛔ 요약 코드 없음 → 스킵")
            return

        # LLM으로 스켈레톤 생성 (Rule 파일 사용)
        result = self.rule_loader.execute(
            role_name='dbms_summarized',
            inputs={
                'summarized_code': summarized,
                'locale': self.locale
            },
            api_key=self.api_key
        )
        skeleton = result['code']

        # 큰 노드도 CONVERSION_BLOCK으로 저장
        original_code = (node.get('node_code') or summarized).strip()
        self._create_and_add_block_query(
            start_line=start_line,
            end_line=end_line,
            original_code=original_code,
            converted_code=skeleton
        )

        entry = {
            'start': start_line,
            'end': end_line,
            'code': skeleton,
            'children': []
        }
        self.parent_stack.append(entry)
        logging.info(
            "      📦 부모 스켈레톤 push | 라인=%s~%s | stack=%s",
            start_line,
            end_line,
            len(self.parent_stack)
        )

    # ----- 소형 노드 처리 -----

    def _handle_small_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        """소형 노드 또는 리프 노드 처리"""
        node_code = (node.get('node_code') or '').strip()
        if not node_code:
            logging.info("    ⛔ 노드 코드 없음 → 스킵")
            return

        # SP 코드 누적
        self.sp_code_parts.append(node_code)
        self.total_tokens = int(self.total_tokens) + int(token)
        logging.info(
            "    ✏️  리프/소형 노드 누적 | 현재 파트 %s개 | 누적 토큰: %s",
            len(self.sp_code_parts),
            self.total_tokens
        )

        # 범위 업데이트
        if self.sp_start is None or start_line < self.sp_start:
            self.sp_start = start_line
        if self.sp_end is None or end_line > self.sp_end:
            self.sp_end = end_line

    async def _flush_pending_accumulation(self, incoming_token: int) -> None:
        """다음 노드 추가 전에 임계값 초과 여부 확인"""
        if (self.sp_code_parts
                and incoming_token is not None
                and (int(self.total_tokens) + int(incoming_token)) >= TOKEN_THRESHOLD):
            logging.info("    ⚠️  다음 노드 추가 시 토큰 초과 예상 → 기존 누적 변환")
            await self._analyze_and_merge()

    # ----- 부모 관리 -----

    async def _finalize_parent(self) -> None:
        """현재 부모 마무리"""
        if not self.parent_stack:
            return

        entry = self.parent_stack.pop()
        logging.info(
            "    ✅ 부모 스켈레톤 pop | 라인=%s~%s | 잔여 children=%s | stack→%s",
            entry['start'],
            entry['end'],
            len(entry['children']),
            len(self.parent_stack)
        )

        code = entry['code']
        child_block = "\n".join(entry['children']).strip()

        if CODE_PLACEHOLDER in code:
            if child_block:
                indented = textwrap.indent(child_block, '    ')
                code = code.replace(CODE_PLACEHOLDER, f"\n{indented}\n", 1)
            else:
                code = code.replace(CODE_PLACEHOLDER, "", 1)
        elif child_block:
            indented = textwrap.indent(child_block, '    ')
            code = f"{code}\n{indented}"

        code = code.strip()

        if self.parent_stack:
            self.parent_stack[-1]['children'].append(code)
            logging.info(
                "      🔁 상위 부모 children에 merge | 상위 라인=%s~%s | stack=%s",
                self.parent_stack[-1]['start'],
                self.parent_stack[-1]['end'],
                len(self.parent_stack)
            )
        else:
            self.merged_code += f"\n{code}"
            logging.info("      🧩 최상위 코드에 병합 완료")

    # ----- 분석 및 병합 -----

    async def _analyze_and_merge(self) -> None:
        """LLM 분석 및 타겟 DBMS 코드 병합"""
        if not self.sp_code_parts or self.sp_start is None:
            return

        # 문자열 조인
        sp_code = '\n'.join(self.sp_code_parts)
        target = "부모 children" if self.parent_stack else "최종코드"
        logging.info(
            "    🤖 LLM 변환 요청 | 라인: %s~%s | 파트 수: %s | 토큰: %s | 대상: %s",
            self.sp_start,
            self.sp_end,
            len(self.sp_code_parts),
            self.total_tokens,
            target
        )

        parent_code = self._build_parent_context()
        logging.debug(
            "      ↳ parent_code 길이=%s | stack=%s",
            len(parent_code),
            len(self.parent_stack)
        )
        result = self.rule_loader.execute(
            role_name='dbms_conversion',
            inputs={
                'code': sp_code,
                'locale': self.locale,
                'parent_code': parent_code
            },
            api_key=self.api_key
        )

        # 생성된 코드 병합
        generated_code = (result.get('code') or '').strip()
        if generated_code:
            # CONVERSION_BLOCK 노드 쿼리 생성
            self._create_and_add_block_query(
                start_line=self.sp_start,
                end_line=self.sp_end,
                original_code=sp_code,
                converted_code=generated_code
            )
            
            if self.parent_stack:
                self.parent_stack[-1]['children'].append(generated_code)
                logging.info(
                    "      ➕ 현재 부모(children) 추가 | 부모 라인=%s~%s | child_len=%s",
                    self.parent_stack[-1]['start'],
                    self.parent_stack[-1]['end'],
                    len(self.parent_stack[-1]['children'])
                )
            else:
                self.merged_code += f"\n{generated_code}"
                logging.info("      ➕ 최종 코드에 변환 결과 추가")

        # 상태 초기화
        self.total_tokens = int(0)
        self.sp_code_parts.clear()
        self.sp_start = None
        self.sp_end = None

    def _build_parent_context(self) -> str:
        """현재 부모 스켈레톤 컨텍스트 구성"""
        if not self.parent_stack:
            return ""

        entry = self.parent_stack[-1]
        return entry['code']

    def _get_current_parent_range(self) -> tuple[int | None, int | None]:
        """현재 부모 범위 반환 (스택의 마지막 항목)"""
        if not self.parent_stack:
            return None, None
        entry = self.parent_stack[-1]
        return entry['start'], entry['end']

    def _calculate_next_relation(self, parent_start: int | None, parent_end: int | None) -> tuple[int | None, int | None]:
        """NEXT 관계 계산
        
        Args:
            parent_start: 부모 시작 라인
            parent_end: 부모 종료 라인
        
        Returns:
            (prev_start, prev_end): 이전 블록 범위 또는 (None, None)
        """
        if not self.last_block_range:
            return None, None
        
        if parent_start is None and parent_end is None:
            # 부모가 없으면 같은 레벨 형제 → NEXT 생성
            return self.last_block_range[0], self.last_block_range[1]
        elif (parent_start is not None and parent_end is not None and
              parent_start < self.last_block_range[0] and 
              self.last_block_range[1] < parent_end):
            # 같은 부모의 형제 노드 → NEXT 생성
            # (last_block_range가 부모 범위 내에 있고, 부모 자체가 아님)
            return self.last_block_range[0], self.last_block_range[1]
        
        return None, None

    def _create_and_add_block_query(
        self,
        start_line: int,
        end_line: int,
        original_code: str,
        converted_code: str
    ) -> None:
        """CONVERSION_BLOCK 쿼리 생성 및 추가"""
        parent_start, parent_end = self._get_current_parent_range()
        prev_start, prev_end = self._calculate_next_relation(parent_start, parent_end)
        
        block_query = build_conversion_block_query(
            folder_name=self.folder_name,
            file_name=self.file_name,
            procedure_name=self.procedure_name,
            user_id=self.user_id,
            start_line=start_line,
            end_line=end_line,
            original_code=original_code,
            converted_code=converted_code,
            conversion_type="dbms",
            target=self.target_dbms,
            parent_start_line=parent_start,
            parent_end_line=parent_end,
            prev_start_line=prev_start,
            prev_end_line=prev_end
        )
        self.conversion_queries.append(block_query)
        self.last_block_range = (start_line, end_line)

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

    async def _save_target_file(self, base_name: str) -> str:
        """타겟 DBMS 파일 자동 저장"""
        try:
            # 저장 경로 설정
            base_path = build_rule_based_path(
                self.project_name,
                self.user_id,
                self.target_dbms,
                'dbms_conversion',
                folder_name=self.folder_name
            )
            
            # 스켈레톤과 병합
            final_code = self.skeleton_code.replace("CodePlaceHolder", self.merged_code.strip())

            # 파일 저장
            await save_file(
                content=final_code,
                filename=f"{base_name}.sql",
                base_path=base_path
            )
            
            logging.info(f"✅ [{base_name}] {self.target_dbms.capitalize()} 파일 자동 저장 완료")
            logging.info(f"📁 저장 경로: {base_path}/{base_name}.sql")
            
            return final_code
            
        except Exception as e:
            logging.error(f"❌ {self.target_dbms.capitalize()} 파일 저장 실패: {str(e)}")
            raise ConvertingError(f"{self.target_dbms.capitalize()} 파일 저장 중 오류: {str(e)}")


# ----- 진입점 함수 -----
async def start_dbms_conversion(
    folder_name: str,
    file_name: str,
    procedure_name: str,
    project_name: str,
    user_id: str,
    api_key: str,
    locale: str,
    target_dbms: str = "oracle"
) -> str:
    """
    DBMS 변환 시작
    
    Args:
        folder_name: 폴더명
        file_name: 파일명
        procedure_name: 프로시저 이름
        project_name: 프로젝트 이름
        user_id: 사용자 ID
        api_key: LLM API 키
        locale: 로케일
        target_dbms: 타겟 DBMS (oracle 등)
    
    Returns:
        str: 변환된 코드
    
    Raises:
        ConvertingError: 변환 중 오류 발생 시
    """
    connection = Neo4jConnection()
    
    logging.info(f"DBMS 변환 시작: {folder_name}/{file_name} (POSTGRES → {target_dbms.upper()})")

    try:
        # Neo4j 쿼리
        query_results = await connection.execute_queries([
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
              
              WITH p
              MATCH (p)-[:PARENT_OF]->(c)
              WHERE NOT c:DECLARE AND NOT c:Table AND NOT c:SPEC
                AND coalesce(toInteger(c.token), 0) >= 1000
              WITH c
              MATCH path = (c)-[:PARENT_OF*0..]->(n)
              WHERE NOT n:DECLARE AND NOT n:Table AND NOT n:SPEC
              WITH n, path, nodes(path) AS pathNodes
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
            """
        ])
        dbms_nodes = query_results[0] if query_results else []

        # 스켈레톤 생성
        skeleton_code = await start_dbms_skeleton(
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            project_name=project_name,
            user_id=user_id,
            api_key=api_key,
            locale=locale,
            target_dbms=target_dbms
        )

        # 변환 수행
        generator = DbmsConversionGenerator(
            dbms_nodes,
            folder_name,
            file_name,
            procedure_name,
            user_id,
            api_key,
            locale,
            project_name,
            target_dbms,
            skeleton_code
        )

        await generator.generate()
        
        # 변환 노드 쿼리들을 Neo4j에 한번에 저장
        if generator.conversion_queries:
            await connection.execute_queries(generator.conversion_queries)
            logging.info(f"✅ 변환 노드 저장 완료: CONVERTING 1개, BLOCK {len(generator.conversion_queries)-1}개")
        
        # 파일 저장
        base_name = file_name.rsplit(".", 1)[0]
        converted_code = await generator._save_target_file(base_name)

        logging.info("\n" + "-"*80)
        logging.info(f"✅ DBMS 변환 완료: {base_name}")
        logging.info("-"*80 + "\n")
        
        return converted_code

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"DBMS 변환 중 오류: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
    finally:
        await connection.close()

