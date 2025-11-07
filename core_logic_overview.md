# Legacy Modernizer 핵심 로직 (소스 코드 중심)

## Understanding 공유 로직

### StatementCollector._visit: AST 평탄화 + StatementNode 생성
AST 노드를 후위 순회하며 `StatementNode`로 변환하고, 부모·자식 연결 및 프로시저 메타 정보를 동시에 구축합니다.
프로시저 루트에서는 이름·스키마를 추출해 `ProcedureInfo`를 초기화하고, 분석 대상 노드는 `pending_nodes`를 증가시켜 요약 완료 여부를 추적합니다.
```245:357:understand/analysis.py
class StatementCollector:
    def _visit(
        self,
        node: Dict[str, Any],
        current_proc: Optional[str],
        current_type: Optional[str],
        current_schema: Optional[str],
    ) -> Optional[StatementNode]:
        start_line = node['startLine']
        end_line = node['endLine']
        node_type = node['type']
        children = node.get('children', []) or []

        child_nodes: List[StatementNode] = []
        procedure_key = current_proc
        procedure_type = current_type
        schema_name = current_schema

        line_entries = [
            (line_no, self._file_lines[line_no - 1] if 0 <= line_no - 1 < len(self._file_lines) else '')
            for line_no in range(start_line, end_line + 1)
        ]
        code = '\n'.join(f"{line_no}: {text}" for line_no, text in line_entries)

        if node_type in PROCEDURE_TYPES:
            schema_candidate, name_candidate = get_procedure_name_from_code(code)
            procedure_key = self._make_proc_key(name_candidate, start_line)
            procedure_type = node_type
            schema_name = schema_candidate
            if procedure_key not in self.procedures:
                self.procedures[procedure_key] = ProcedureInfo(
                    key=procedure_key,
                    procedure_type=node_type,
                    procedure_name=name_candidate or procedure_key,
                    schema_name=schema_candidate,
                    start_line=start_line,
                    end_line=end_line,
                )

        for child in children:
            child_node = self._visit(child, procedure_key, procedure_type, schema_name)
            if child_node is not None:
                child_nodes.append(child_node)

        analyzable = node_type not in NON_ANALYSIS_TYPES
        token = calculate_code_token(code)
        dml = node_type in DML_STATEMENT_TYPES
        has_children = bool(child_nodes)

        self._node_id += 1
        statement_node = StatementNode(
            node_id=self._node_id,
            start_line=start_line,
            end_line=end_line,
            node_type=node_type,
            code=code,
            token=token,
            has_children=has_children,
            procedure_key=procedure_key,
            procedure_type=procedure_type,
            procedure_name=self.procedures.get(procedure_key).procedure_name if procedure_key in self.procedures else None,
            schema_name=schema_name,
            analyzable=analyzable,
            dml=dml,
            lines=line_entries,
        )
        for child_node in child_nodes:
            child_node.parent = statement_node
        statement_node.children.extend(child_nodes)

        if analyzable and procedure_key and procedure_key in self.procedures:
            self.procedures[procedure_key].pending_nodes += 1
        else:
            statement_node.completion_event.set()

        self.nodes.append(statement_node)
        return statement_node
```

### BatchPlanner.plan: 토큰 기반 배치 설계
수집된 `StatementNode`를 토큰 합계와 부모 여부에 따라 분리해 LLM 호출 단위를 형성합니다.
부모 노드는 단독 배치로, 리프 노드는 토큰 한도(`MAX_BATCH_TOKEN`)를 넘지 않는 범위에서 묶습니다.
```360:431:understand/analysis.py
class BatchPlanner:
    def plan(self, nodes: List[StatementNode], folder_file: str) -> List[AnalysisBatch]:
        batches: List[AnalysisBatch] = []
        current_nodes: List[StatementNode] = []
        current_tokens = 0
        batch_id = 1

        for node in nodes:
            if not node.analyzable:
                continue

            if node.has_children:
                if current_nodes:
                    batches.append(self._create_batch(batch_id, current_nodes))
                    batch_id += 1
                    current_nodes = []
                    current_tokens = 0

                batches.append(self._create_batch(batch_id, [node]))
                batch_id += 1
                continue

            if current_nodes and current_tokens + node.token > self.token_limit:
                batches.append(self._create_batch(batch_id, current_nodes))
                batch_id += 1
                current_nodes = []
                current_tokens = 0

            current_nodes.append(node)
            current_tokens += node.token

        if current_nodes:
            batches.append(self._create_batch(batch_id, current_nodes))

        return batches
```

### ApplyManager._apply_batch: LLM 결과 → Neo4j 반영
LLM 응답을 받아 요약·변수 사용·CALL 관계를 Cypher 쿼리로 생성하고, 배치 순서를 보장하며 Neo4j에 전송합니다.
또한 프로시저 요약 버킷을 갱신해 모든 노드가 처리되면 후속 요약 작업을 트리거합니다.
```535:688:understand/analysis.py
class ApplyManager:
    async def _apply_batch(self, result: BatchResult):
        if not result.general_result:
            general_items: List[Dict[str, Any]] = []
        else:
            general_items = result.general_result.get('analysis', [])

        cypher_queries: List[str] = []
        summary_nodes = list(zip(result.batch.nodes, general_items))
        processed_nodes: set[int] = set()

        for node, analysis in summary_nodes:
            if not analysis:
                node.completion_event.set()
                continue
            cypher_queries.extend(self._build_node_queries(node, analysis))
            self._update_summary_store(node, analysis)
            processed_nodes.add(node.node_id)

        for node in result.batch.nodes:
            if node.node_id not in processed_nodes and node.completion_event.is_set() is False:
                node.completion_event.set()

        if result.table_result:
            cypher_queries.extend(self._build_table_queries(result.batch, result.table_result))

        if cypher_queries:
            await self._send_queries(cypher_queries, result.batch.progress_line)
```

### Analyzer.run: 파이프라인 오케스트레이션
파일 단위 Understanding 전체를 제어하는 엔트리 포인트로, 수집→그래프 초기화→배치 실행→결과 적용을 비동기로 연결합니다.
자식 요약 완료를 기다린 뒤 배치별로 LLM을 호출하고 결과를 ApplyManager에 전달해 순차 처리합니다.
```1252:1316:understand/analysis.py
class Analyzer:
    async def run(self):
        logging.info("[진행] %s 분석 시작 (총 %s줄)", self.folder_file, self.last_line)
        try:
            collector = StatementCollector(self.antlr_data, self.file_content, self.folder_name, self.file_name)
            nodes, procedures = collector.collect()
            await self._initialize_static_graph(nodes)
            planner = BatchPlanner()
            batches = planner.plan(nodes, self.folder_file)

            if not batches:
                await self.send_queue.put({"type": "end_analysis"})
                return

            invoker = LLMInvoker(self.api_key, self.locale)
            apply_manager = ApplyManager(
                node_base_props=self.node_base_props,
                folder_props=self.folder_props,
                table_base_props=self.table_base_props,
                user_id=self.user_id,
                project_name=self.project_name,
                folder_name=self.folder_name,
                file_name=self.file_name,
                dbms=self.dbms,
                api_key=self.api_key,
                locale=self.locale,
                procedures=procedures,
                send_queue=self.send_queue,
                receive_queue=self.receive_queue,
                file_last_line=self.last_line,
            )

            semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))

            async def worker(batch: AnalysisBatch):
                await self._wait_for_dependencies(batch)
                async with semaphore:
                    general, table = await invoker.invoke(batch)
                await apply_manager.submit(batch, general, table)

            await asyncio.gather(*(worker(batch) for batch in batches))
            await apply_manager.finalize()

            await self.send_queue.put({"type": "end_analysis"})
```


## Converting 공유 로직

### DbmsConversionGenerator.generate & _process_node: 그래프 순회 + 토큰 컨텍스트 관리
Neo4j에서 조회한 노드를 순회하며 토큰 버퍼·부모/자식·TRY 상태를 추적하는 DBMS 변환 메인 루프입니다.
대용량 노드는 LLM 스켈레톤으로 처리하고, 임계 토큰을 넘으면 누적 코드를 분석·병합합니다.
```62:138:convert/create_dbms_conversion.py
class DbmsConversionGenerator:
    async def generate(self) -> str:
        logging.info(f"📋 DBMS 변환 노드 순회 시작: postgres → {self.target_dbms}")
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
        return self.merged_code.strip()

    async def _process_node(self, record: dict) -> None:
        node = record['n']
        node_labels = record.get('nodeLabels', [])
        node_type = node_labels[0] if node_labels else node.get('name', 'UNKNOWN')
        has_children = bool(node.get('has_children', False))
        token = int(node.get('token', 0) or 0)
        start_line = int(node.get('startLine', 0) or 0)
        end_line = int(node.get('endLine', 0) or 0)
        relationship = record['r'][1] if record.get('r') else 'NEXT'

        if node_type == 'TRY':
            self.pending_try_mode = True

        if node_type == 'EXCEPTION':
            await self._handle_exception_node(node, start_line, end_line)
            return

        parent = self.current_parent
        if parent and relationship == 'NEXT' and start_line > parent['end']:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            await self._finalize_parent()

        if token >= TOKEN_THRESHOLD and has_children and node_type not in DML_TYPES:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            await self._handle_large_node(node, start_line, end_line, token)
        else:
            self._handle_small_node(node, start_line, end_line, token)

        if int(self.total_tokens) >= TOKEN_THRESHOLD:
            await self._analyze_and_merge()
```

### DbmsConversionGenerator._handle_large_node → _analyze_and_merge: LLM 기반 코드 합성
요약된 부모 범위를 기반으로 스켈레톤을 삽입한 뒤, 누적된 리프 코드를 LLM에 전달해 실제 변환 코드를 얻습니다.
TRY 블록 여부에 따라 코드 버퍼를 분기 처리해 예외 처리 구간을 보존합니다.
```141:210:convert/create_dbms_conversion.py
    async def _handle_large_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        summarized = (node.get('summarized_code') or '').strip()
        if not summarized:
            return

        result = self.rule_loader.execute(
            role_name='dbms_summarized',
            inputs={
                'summarized_code': summarized,
                'locale': self.locale
            },
            api_key=self.api_key
        )
        skeleton = result['code']

        if not self.current_parent:
            self.current_parent = {'start': start_line, 'end': end_line, 'code': skeleton}
        else:
            self.current_parent['code'] = self.current_parent['code'].replace(
                CODE_PLACEHOLDER, f"\n{textwrap.indent(skeleton, '    ')}", 1
            )

    async def _analyze_and_merge(self) -> None:
        if not self.sp_code_parts or self.sp_start is None:
            return

        sp_code = '\n'.join(self.sp_code_parts)
        result = self.rule_loader.execute(
            role_name='dbms_conversion',
            inputs={
                'code': sp_code,
                'locale': self.locale,
                'parent_code': self.current_parent['code'] if self.current_parent else ""
            },
            api_key=self.api_key
        )

        generated_code = (result.get('code') or '').strip()
        if generated_code:
            if self.current_parent:
                self.code_buffer += f"\n{generated_code}"
            else:
                if self.pending_try_mode:
                    self.code_buffer += f"\n{generated_code}"
                else:
                    self.merged_code += f"\n{generated_code}"

        self.total_tokens = int(0)
        self.sp_code_parts.clear()
        self.sp_start = None
        self.sp_end = None
```

### ServicePreprocessingGenerator._process_node: 서비스 코드 변환 파이프라인
DBMS 변환과 동일한 컨텍스트 관리 패턴으로 자바 서비스 생성에 맞게 노드를 분기 처리합니다.
TRY/EXCEPTION 상태, 토큰 임계, 부모 경계를 감지하며 LLM 분석 타이밍을 제어합니다.
```97:188:convert/create_service_preprocessing.py
class ServicePreprocessingGenerator:
    async def _process_node(self, record: dict) -> None:
        node = record['n']
        node_labels = record.get('nodeLabels', [])
        node_type = node_labels[0] if node_labels else node.get('name', 'UNKNOWN')
        has_children = bool(node.get('has_children', False))
        token = int(node.get('token', 0) or 0)
        start_line = int(node.get('startLine', 0) or 0)
        end_line = int(node.get('endLine', 0) or 0)
        relationship = record['r'][1] if record.get('r') else 'NEXT'

        if node_type == 'TRY':
            self.pending_try_mode = True

        if node_type == 'EXCEPTION':
            await self._handle_exception_node(node, start_line, end_line)
            return

        parent = self.current_parent
        if parent and relationship == 'NEXT' and start_line > parent['end']:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            await self._finalize_parent()

        if token >= TOKEN_THRESHOLD and has_children and node_type not in DML_TYPES:
            if self.sp_code_parts:
                await self._analyze_and_merge()
            logging.info(f"  ┌─ 큰 노드 진입 [{start_line}~{end_line}] (토큰: {token})")
            await self._handle_large_node(node, start_line, end_line, token)
        else:
            self._handle_small_node(node, start_line, end_line, token)

        if int(self.total_tokens) >= TOKEN_THRESHOLD:
            await self._analyze_and_merge()
```

### ServicePreprocessingGenerator._handle_large_node: 서비스 스켈레톤 합성
Neo4j에서 수집한 변수·쿼리 맥락과 요약 코드를 LLM에 전달해 서비스 메서드 스켈레톤을 갱신합니다.
부모 코드의 placeholder를 채우며 Command/Sequence 정보도 함께 반영합니다.
```149:184:convert/create_service_preprocessing.py
    async def _handle_large_node(self, node: dict, start_line: int, end_line: int, token: int) -> None:
        summarized = (node.get('summarized_code') or '').strip()
        if not summarized:
            return

        used_vars, used_queries = await self._collect_current_context()

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

        if not self.current_parent:
            self.current_parent = {'start': start_line, 'end': end_line, 'code': skeleton}
        else:
            self.current_parent['code'] = self.current_parent['code'].replace(
                CODE_PLACEHOLDER, f"\n{textwrap.indent(skeleton, '    ')}", 1
            )
```

### ServicePreprocessingGenerator._analyze_and_merge: 서비스 메서드 본문 생성
누적된 리프 코드와 수집한 변수/쿼리 메타를 결합해 `service` 룰을 호출하고, 실제 자바 메서드 본문을 생성합니다.
생성된 코드와 변수 역할 정보를 각각 버퍼와 `tracking_variables`에 반영합니다.
```321:383:convert/create_service_preprocessing.py
    async def _analyze_and_merge(self) -> None:
        if not self.sp_code_parts or self.sp_start is None:
            return

        sp_code = '\n'.join(self.sp_code_parts)
        used_variables = []
        try:
            collected = await collect_variables_in_range(self.variable_nodes, self.sp_start, self.sp_end)
            used_variables = [{**v, 'role': self.tracking_variables.get(v['name'], '')} for v in collected]
        except Exception as e:
            logging.debug(f"변수 수집 스킵: {e}")

        used_query_methods = {}
        try:
            used_query_methods = await extract_used_query_methods(
                self.sp_start, self.sp_end, self.query_method_list, {}
            )
        except Exception as e:
            logging.debug(f"JPA 수집 스킵: {e}")

        result = self.rule_loader.execute(
            role_name='service',
            inputs={
                'code': sp_code,
                'service_skeleton': json.dumps(self.service_skeleton, ensure_ascii=False),
                'variable': json.dumps(used_variables, ensure_ascii=False, indent=2),
                'query_method_list': json.dumps(used_query_methods, ensure_ascii=False, indent=2),
                'sequence_methods': json.dumps(self.sequence_methods, ensure_ascii=False, indent=2),
                'locale': self.locale,
                'parent_code': self.current_parent['code'] if self.current_parent else ""
            },
            api_key=self.api_key
        )

        self.tracking_variables.update(result['analysis'].get('variables', {}))

        java_code = (result.get('analysis', {}).get('code') or '').strip()
        if java_code:
            if self.current_parent:
                self.java_buffer += f"\n{java_code}"
            else:
                if self.pending_try_mode:
                    self.java_buffer += f"\n{java_code}"
                else:
                    self.merged_java_code += f"\n{java_code}"

        self.total_tokens = int(0)
        self.sp_code_parts.clear()
        self.sp_start = None
        self.sp_end = None
```


> 위 정리는 Understanding/Converting 공통 로직을 구성하는 실제 소스 코드 조각을 그대로 발췌해 핵심 알고리즘 흐름을 문서화한 것입니다.

