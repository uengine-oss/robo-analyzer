from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from understand.rules import (
    understand_code,
    understand_dml_tables,
    summarize_table_metadata,
    understand_summary,
)
from util.utility_tool import escape_for_cypher, parse_table_identifier, log_process

# DBMS 전용 상수 (전략 내부에 보관)
DBMS_PROCEDURE_TYPES = (
    "PROCEDURE",
    "FUNCTION",
    "CREATE_PROCEDURE_BODY",
    "TRIGGER",
)

DBMS_NON_ANALYSIS_TYPES = frozenset([
    "CREATE_PROCEDURE_BODY",
    "FILE",
    "PROCEDURE",
    "FUNCTION",
    "DECLARE",
    "TRIGGER",
    "FOLDER",
    "SPEC",
])

DBMS_NON_NEXT_RECURSIVE_TYPES = frozenset([
    "FUNCTION",
    "PROCEDURE",
    "PACKAGE_VARIABLE",
    "TRIGGER",
])

DBMS_DML_STATEMENT_TYPES = frozenset([
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "EXECUTE_IMMEDIATE",
    "FETCH",
    "CREATE_TEMP_TABLE",
    "CTE",
    "OPEN_CURSOR",
])

DBMS_VARIABLE_ROLE_MAP = {
    "PACKAGE_VARIABLE": "패키지 전역 변수",
    "DECLARE": "변수 선언및 초기화",
    "SPEC": "함수 및 프로시저 입력 매개변수",
}

DBMS_VARIABLE_DECLARATION_TYPES = frozenset([
    "PACKAGE_VARIABLE",
    "DECLARE",
    "SPEC",
])

DBMS_TABLE_RELATIONSHIP_MAP = {
    "r": "FROM",
    "w": "WRITES",
}


class DbmsUnderstandingInvoker:
    """DBMS/SP용 LLM 호출기.

    - __init__: API 키/로케일만 보관해 배치마다 재사용
    - invoke: 일반 요약과 테이블 요약을 병렬로 요청하여 결과 튜플 반환
    """

    def __init__(self, api_key: str, locale: str):
        """API 키/로케일을 저장해 배치 호출 시 재사용합니다."""
        self.api_key = api_key
        self.locale = locale

    async def invoke(self, batch) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """일반 요약과 테이블 요약을 병렬로 호출한 결과를 반환합니다."""
        general_task = None
        if batch.ranges:
            general_task = asyncio.to_thread(
                understand_code,
                batch.build_general_payload(),
                batch.ranges,
                len(batch.ranges),
                self.api_key,
                self.locale,
            )

        table_task = None
        dml_payload = batch.build_dml_payload()
        if dml_payload and batch.dml_ranges:
            table_task = asyncio.to_thread(
                understand_dml_tables,
                dml_payload,
                batch.dml_ranges,
                self.api_key,
                self.locale,
            )

        if general_task and table_task:
            return await asyncio.gather(general_task, table_task)
        if general_task:
            return await general_task, None
        if table_task:
            return None, await table_task
        return None, None


class DbmsUnderstandingApplyManager:
    """DBMS/SP용 적용 관리자.

    - submit: 순서가 보장된 배치 결과 큐에 적재
    - finalize: 남은 배치를 모두 적용한 뒤 프로시저/테이블 요약까지 마무리
    - _apply_batch: 요약 결과를 Cypher로 만들어 전송하고 완료 이벤트 설정
    """

    def __init__(
        self,
        node_base_props: str,
        folder_props: str,
        table_base_props: str,
        user_id: str,
        project_name: str,
        folder_name: str,
        file_name: str,
        dbms: str,
        api_key: str,
        locale: str,
        procedures: Dict[str, Any],
        send_queue: asyncio.Queue,
        receive_queue: asyncio.Queue,
        file_last_line: int,
    ):
        """적용 단계에 필요한 메타데이터와 큐를 보관합니다."""
        self.node_base_props = node_base_props
        self.folder_props = folder_props
        self.table_base_props = table_base_props
        self.user_id = user_id
        self.project_name = project_name
        self.folder_name = folder_name
        self.file_name = file_name
        self.dbms = dbms
        self.api_key = api_key
        self.locale = locale
        self.procedures = procedures
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.file_last_line = file_last_line
        self.folder_file = f"{folder_name}-{file_name}"

        self._pending: Dict[int, Any] = {}
        self._summary_store: Dict[str, Dict[str, Any]] = {key: {} for key in procedures}
        self._next_batch_id = 1
        self._lock = asyncio.Lock()
        self._table_summary_store: Dict[Tuple[str, str], Dict[str, Any]] = {}

    async def submit(self, batch, general: Optional[Dict[str, Any]], table: Optional[Dict[str, Any]]):
        """배치 결과를 보관 후 적용 가능 상태가 되면 즉시 처리."""
        async with self._lock:
            self._pending[batch.batch_id] = {"batch": batch, "general": general, "table": table}
            await self._flush_ready()

    async def finalize(self):
        """모든 배치를 강제로 플러시하고 요약 후처리를 끝낸다."""
        async with self._lock:
            await self._flush_ready(force=True)
        await self._finalize_remaining_procedures()
        await self._finalize_table_summaries()

    async def _flush_ready(self, force: bool = False):
        """배치 ID 순서대로 적용 가능한 항목을 처리한다."""
        while self._next_batch_id in self._pending:
            result = self._pending.pop(self._next_batch_id)
            await self._apply_batch(result)
            self._next_batch_id += 1

        if force and self._pending:
            for batch_id in sorted(self._pending):
                result = self._pending.pop(batch_id)
                await self._apply_batch(result)

    async def _apply_batch(self, result: Dict[str, Any]):
        """LLM 결과를 Cypher로 변환하고 완료 이벤트를 설정한다."""
        batch = result["batch"]
        general_result = result.get("general")
        table_result = result.get("table")

        general_items: List[Dict[str, Any]] = general_result.get('analysis', []) if general_result else []

        cypher_queries: List[str] = []
        summary_nodes = list(zip(batch.nodes, general_items))
        processed_nodes: set[int] = set()

        for node, analysis in summary_nodes:
            if not analysis:
                log_process("UNDERSTAND", "APPLY", f"⚠️ LLM이 {node.start_line}~{node.end_line} 구간에 요약을 반환하지 않음 - 건너뜀")
                node.completion_event.set()
                continue

            # 노드 요약/코멘트 반영
            cypher_queries.extend(self._build_summary_queries(node, analysis))
            processed_nodes.add(node.node_id)

            # 프로시저 요약 후보 저장 및 완료 판단
            self._update_summary_store(node, analysis)

            # 부모 compact 코드 생성 시 요약 누락 로그가 뜨지 않도록 메모리에 즉시 반영
            node.summary = (analysis.get("summary") or "").strip() or None

            # CALL / Variable 사용 반영
            cypher_queries.extend(self._build_call_queries(node, analysis))
            cypher_queries.extend(self._build_variable_usage_queries(node, analysis))
            cypher_queries.extend(self._apply_table_analysis(analysis))

        # 테이블/DML 분석 결과 반영 (별도 DML 프롬프트 결과 활용)
        if table_result:
            cypher_queries.extend(self._build_table_queries(batch, table_result))

        if cypher_queries:
            await self._send_queries(cypher_queries, batch.progress_line)

        # 요약 완료 이벤트 설정
        for node in batch.nodes:
            if node.node_id in processed_nodes:
                node.completion_event.set()

    def _build_variable_usage_queries(self, node, analysis: Dict[str, Any]) -> List[str]:
        """요약 결과 기반으로 Variable 노드에 사용 흔적을 마킹합니다."""
        queries: List[str] = []
        for var_name in analysis.get('variables', []) or []:
            escaped_var = escape_for_cypher(var_name)
            queries.append(
                f"MATCH (v:Variable {{name: '{escaped_var}', {self.node_base_props}}})\n"
                f"SET v.`{node.start_line}_{node.end_line}` = 'Used'"
            )
        return queries

    def _build_table_queries(self, batch, table_result: Dict[str, Any]) -> List[str]:
        """별도 DML 프롬프트 결과를 기반으로 테이블/컬럼/관계 쿼리를 생성합니다."""
        if not table_result:
            return []

        queries: List[str] = []
        node_map: Dict[Tuple[int, int], Any] = {
            (node.start_line, node.end_line): node for node in batch.nodes
        }

        normalized_ranges: List[Dict[str, Any]] = list(table_result.get('ranges', []) or [])
        for legacy_entry in table_result.get('tables', []) or []:
            normalized_ranges.append({
                "startLine": legacy_entry.get('startLine'),
                "endLine": legacy_entry.get('endLine'),
                "tables": [legacy_entry],
            })

        for range_entry in normalized_ranges:
            try:
                start_line = int(range_entry.get('startLine'))
                end_line = int(range_entry.get('endLine'))
            except (TypeError, ValueError):
                continue

            node = node_map.get((start_line, end_line))
            if not node:
                continue

            tables = range_entry.get('tables') or []
            node_merge_base = f"MERGE (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"

            if node.node_type == 'CREATE_TEMP_TABLE':
                for entry in tables:
                    table_name = (entry.get('table') or '').strip()
                    if not table_name:
                        continue
                    schema_part, name_part, _ = parse_table_identifier(table_name)
                    queries.append(
                        f"{node_merge_base}\n"
                        f"SET n:Table, n.name = '{escape_for_cypher(name_part)}', n.schema = '{escape_for_cypher(schema_part)}', "
                        f"n.db = '{self.dbms}'"
                    )
                continue

            for entry in tables:
                table_name = (entry.get('table') or '').strip()
                if not table_name:
                    continue

                schema_part, name_part, db_link_value = parse_table_identifier(table_name)
                access_mode_raw = (entry.get('accessMode') or '').lower()
                relationship_targets: List[str] = []
                if 'r' in access_mode_raw:
                    relationship_targets.append(DBMS_TABLE_RELATIONSHIP_MAP['r'])
                if 'w' in access_mode_raw:
                    relationship_targets.append(DBMS_TABLE_RELATIONSHIP_MAP['w'])

                table_merge = self._build_table_merge(name_part, schema_part)
                folder_merge = f"MERGE (folder:SYSTEM {{{self.folder_props}}})"
                bucket_key = self._record_table_summary(schema_part, name_part, entry.get('tableDescription'))

                base_table_query = (
                    f"{node_merge_base}\n"
                    f"WITH n\n"
                    f"{table_merge}\n"
                    f"WITH n, t\n"
                    f"{folder_merge}\n"
                    f"MERGE (folder)-[:CONTAINS]->(t)\n"
                    f"SET t.db = coalesce(t.db, '{self.dbms}')"
                )

                if db_link_value:
                    base_table_query += f"\nSET t.db_link = COALESCE(t.db_link, '{db_link_value}')"

                for relationship in relationship_targets:
                    base_table_query += f"\nMERGE (n)-[:{relationship}]->(t)"

                queries.append(base_table_query)

                for column in entry.get('columns', []) or []:
                    column_name = (column.get('name') or '').strip()
                    if not column_name:
                        continue
                    raw_dtype = (column.get('dtype') or '')
                    col_type = escape_for_cypher(raw_dtype or '')
                    raw_column_desc = (column.get('description') or column.get('comment') or '').strip()

                    self._record_column_summary(
                        bucket_key,
                        column_name,
                        raw_column_desc,
                        dtype=raw_dtype,
                        nullable=column.get('nullable', True),
                        examples=(column.get('examples') or [])
                    )

                    col_description = escape_for_cypher(raw_column_desc)
                    nullable_flag = 'true' if column.get('nullable', True) else 'false'
                    escaped_column_name = escape_for_cypher(column_name)

                    if schema_part:
                        fqn = '.'.join(filter(None, [schema_part, name_part, column_name])).lower()
                        column_merge_key = f"`user_id`: '{self.user_id}', `fqn`: '{fqn}', `project_name`: '{self.project_name}'"
                        queries.append(
                            f"{table_merge}\n"
                            f"WITH t\n"
                            f"MERGE (c:Column {{{column_merge_key}}})\n"
                            f"SET c.`name` = '{escaped_column_name}', c.`dtype` = '{col_type}', c.`description` = '{col_description}', c.`nullable` = '{nullable_flag}', c.`fqn` = '{fqn}'\n"
                            f"WITH t, c\n"
                            f"MERGE (t)-[:HAS_COLUMN]->(c)"
                        )
                    else:
                        queries.append(
                            f"{table_merge}\n"
                            f"WITH t\n"
                            f"OPTIONAL MATCH (existing_col:Column)-[:HAS_COLUMN]-(t)\n"
                            f"WHERE existing_col.`name` = '{escaped_column_name}' AND existing_col.`user_id` = '{self.user_id}' AND existing_col.`project_name` = '{self.project_name}'\n"
                            f"WITH t, existing_col\n"
                            f"WHERE existing_col IS NULL\n"
                            f"WITH t, "
                            f"lower(case when t.schema <> '' and t.schema IS NOT NULL then t.schema + '.' + '{name_part}' + '.' + '{column_name}' else '{name_part}' + '.' + '{column_name}' end) as fqn\n"
                            f"CREATE (c:Column {{`user_id`: '{self.user_id}', `fqn`: fqn, `project_name`: '{self.project_name}', "
                            f"`name`: '{escaped_column_name}', `dtype`: '{col_type}', `description`: '{col_description}', `nullable`: '{nullable_flag}'}})\n"
                            f"WITH t, c\n"
                            f"MERGE (t)-[:HAS_COLUMN]->(c)"
                        )

            for link_item in range_entry.get('dbLinks', []) or []:
                link_name_raw = (link_item.get('name') or '').strip()
                if not link_name_raw:
                    continue
                mode = (link_item.get('mode') or 'r').lower()
                schema_link, name_link, link_name = parse_table_identifier(link_name_raw)
                remote_merge = self._build_table_merge(name_link, schema_link).replace(f", db: '{self.dbms}'", "")
                queries.append(
                    f"{remote_merge}\n"
                    f"SET t.db_link = '{link_name}'\n"
                    f"WITH t\n"
                    f"MERGE (l:DBLink {{user_id: '{self.user_id}', name: '{link_name}', project_name: '{self.project_name}'}})\n"
                    f"MERGE (l)-[:CONTAINS]->(t)\n"
                    f"WITH t\n"
                    f"{node_merge_base}\n"
                    f"MERGE (n)-[:DB_LINK {{mode: '{mode}'}}]->(t)"
                )

            for relation in range_entry.get('fkRelations', []) or []:
                src_table = (relation.get('sourceTable') or '').strip()
                tgt_table = (relation.get('targetTable') or '').strip()
                src_columns = [
                    (column or '').strip()
                    for column in (relation.get('sourceColumns') or [])
                    if column is not None and str(column).strip()
                ]
                tgt_columns = [
                    (column or '').strip()
                    for column in (relation.get('targetColumns') or [])
                    if column is not None and str(column).strip()
                ]
                if not (src_table and tgt_table and src_columns and tgt_columns):
                    continue
                src_schema, src_table_name, _ = parse_table_identifier(src_table)
                tgt_schema, tgt_table_name, _ = parse_table_identifier(tgt_table)
                src_props = (
                    f"user_id: '{self.user_id}', schema: '{src_schema or ''}', name: '{src_table_name}', db: '{self.dbms}', project_name: '{self.project_name}'"
                )
                tgt_props = (
                    f"user_id: '{self.user_id}', schema: '{tgt_schema or ''}', name: '{tgt_table_name}', db: '{self.dbms}', project_name: '{self.project_name}'"
                )
                queries.append(
                    f"MATCH (st:Table {{{src_props}}})\n"
                    f"MATCH (tt:Table {{{tgt_props}}})\n"
                    f"MERGE (st)-[:FK_TO_TABLE]->(tt)"
                )
                for src_column, tgt_column in zip(src_columns, tgt_columns):
                    if not (src_column and tgt_column):
                        continue
                    src_fqn = '.'.join(filter(None, [src_schema, src_table_name, src_column])).lower()
                    tgt_fqn = '.'.join(filter(None, [tgt_schema, tgt_table_name, tgt_column])).lower()
                    queries.append(
                        f"MATCH (sc:Column {{user_id: '{self.user_id}', name: '{src_column}', fqn: '{src_fqn}'}})\n"
                        f"MATCH (dc:Column {{user_id: '{self.user_id}', name: '{tgt_column}', fqn: '{tgt_fqn}'}})\n"
                        f"MERGE (sc)-[:FK_TO]->(dc)"
                    )

        return queries

    def _build_summary_queries(self, node, analysis: Dict[str, Any]) -> List[str]:
        """노드 요약 및 세부 설명을 Cypher 쿼리로 변환합니다."""
        summary_text = (analysis.get('summary') or '').strip()
        summary_json = json.dumps(analysis.get('summaryJson') or "", ensure_ascii=False)
        insights_json = json.dumps(analysis.get('insights') or "", ensure_ascii=False)

        queries: List[str] = []
        summary_fragment = ""
        if summary_text:
            summary_fragment = f", n.summary = '{escape_for_cypher(summary_text)}'"
        queries.append(
            f"MATCH (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
            f"SET n.summaryJson = {summary_json}{summary_fragment}, n.insights = {insights_json}"
        )

        # 상세 요약/설명 처리
        detail_text = analysis.get('detail') or analysis.get('detailSummary') or ''
        if isinstance(detail_text, str) and detail_text.strip():
            queries.append(
                f"MATCH (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET n.detail = '{escape_for_cypher(detail_text.strip())}'"
            )

        return queries

    def _update_summary_store(self, node, analysis: Dict[str, Any]):
        """프로시저 요약 후보를 저장하고 완료된 노드 수를 갱신합니다."""
        if not node.procedure_key or node.procedure_key not in self.procedures:
            return
        summary_entry = analysis.get('summary')
        if summary_entry is None:
            return
        key = f"{node.node_type}_{node.start_line}_{node.end_line}"
        self._summary_store[node.procedure_key][key] = summary_entry
        info = self.procedures[node.procedure_key]
        if info.pending_nodes > 0:
            info.pending_nodes -= 1
        if info.pending_nodes == 0:
            asyncio.create_task(self._finalize_procedure_summary(info))

    def _apply_table_analysis(self, analysis: Dict[str, Any]) -> List[str]:
        """일반 요약 결과에 포함된 테이블 관계를 Procedure-Table 관계로 반영합니다."""
        queries: List[str] = []
        table_analysis = analysis.get('tables')
        if not table_analysis:
            return queries

        for table_entry in table_analysis:
            table_name = table_entry.get("table")
            if not table_name:
                continue

            relationship_targets: List[str] = []
            rel_raw = table_entry.get("relationship")
            if rel_raw:
                if 'r' in rel_raw:
                    relationship_targets.append(DBMS_TABLE_RELATIONSHIP_MAP['r'])
                if 'w' in rel_raw:
                    relationship_targets.append(DBMS_TABLE_RELATIONSHIP_MAP['w'])

            schema_part, name_part, db_link_value = parse_table_identifier(table_name)
            link_fragment = f", db_link: '{escape_for_cypher(db_link_value)}'" if db_link_value else ""
            table_props = (
                f"user_id: '{self.user_id}', schema: '{escape_for_cypher(schema_part)}', name: '{escape_for_cypher(name_part)}', "
                f"db: '{self.dbms}', project_name: '{self.project_name}'{link_fragment}"
            )

            for relation_type in relationship_targets:
                queries.append(
                    f"MATCH (p:Procedure {{name: '{escape_for_cypher(table_entry.get('procedure') or '')}', user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
                    f"MATCH (t:Table {{{table_props}}})\n"
                    f"MERGE (p)-[:{relation_type}]->(t)"
                )

        return queries

    async def _finalize_remaining_procedures(self):
        """남은 프로시저 요약 후보를 모두 처리합니다."""
        tasks = []
        for key, info in list(self.procedures.items()):
            if info.pending_nodes == 0 and key in self._summary_store and self._summary_store[key]:
                tasks.append(self._finalize_procedure_summary(info))
        if tasks:
            await asyncio.gather(*tasks)

    async def _finalize_procedure_summary(self, info):
        """프로시저 수준 요약을 LLM으로 생성하고 Neo4j에 반영합니다."""
        if info.key not in self._summary_store:
            return
        summaries = self._summary_store.pop(info.key, {})
        if not summaries:
            return
        try:
            summary_result = await asyncio.to_thread(
                understand_summary,
                summaries,
                self.api_key,
                self.locale,
            )
        except Exception as exc:  # pragma: no cover - defensive
            log_process("UNDERSTAND", "SUMMARY", f"❌ {info.procedure_name} 프로시저 요약 생성 중 오류 발생", logging.ERROR, exc)
            return

        summary_value = summary_result.get('summary') if isinstance(summary_result, dict) else None
        if summary_value is None:
            return

        summary_json = json.dumps(summary_value, ensure_ascii=False)
        query = (
            f"MATCH (n:{info.procedure_type} {{procedure_name: '{escape_for_cypher(info.procedure_name)}', "
            f"user_id: '{self.user_id}', project_name: '{self.project_name}', db: '{self.dbms}'}})\n"
            f"SET n.summary = {summary_json}"
        )
        await self._send_queries([query], info.end_line)
        log_process("UNDERSTAND", "SUMMARY", f"✅ {info.procedure_name} 프로시저 요약을 Neo4j에 반영 완료")

    async def _finalize_table_summaries(self):
        """누적된 테이블/컬럼 설명을 요약하고 Neo4j에 반영합니다."""
        if not self._table_summary_store:
            return
        tasks = [
            self._summarize_table(table_key, data)
            for table_key, data in list(self._table_summary_store.items())
        ]
        if tasks:
            await asyncio.gather(*tasks)
        self._table_summary_store.clear()

    async def _summarize_table(self, table_key: Tuple[str, str], data: Dict[str, Any]):
        """하나의 테이블에 대한 누적 설명/컬럼 정보를 요약합니다."""
        schema_key, name_key = table_key
        summaries = list(data.get('summaries') or [])
        columns_map = data.get('columns') or {}
        column_sentences = {
            entry['name']: list(entry['summaries'])
            for entry in columns_map.values()
            if entry.get('summaries')
        }
        if not summaries and not column_sentences:
            return

        table_display = f"{schema_key}.{name_key}" if schema_key else name_key
        column_metadata = {
            entry['name']: {
                "dtype": entry.get("dtype") or "",
                "nullable": bool(entry.get("nullable", True)),
                "examples": sorted(list(entry.get("examples") or []))[:5],
            }
            for entry in columns_map.values()
        }

        result = await asyncio.to_thread(
            summarize_table_metadata,
            table_display,
            summaries,
            column_sentences,
            column_metadata,
            self.api_key,
            self.locale,
        )

        if not isinstance(result, dict):
            return

        queries: List[str] = []
        table_desc = (result.get('tableDescription') or '').strip()
        schema_prop = schema_key
        table_props = (
            f"user_id: '{self.user_id}', schema: '{schema_prop}', name: '{name_key}', db: '{self.dbms}', project_name: '{self.project_name}'"
        )

        if table_desc:
            queries.append(
                f"MATCH (t:Table {{{table_props}}})\nSET t.description = '{escape_for_cypher(table_desc)}'"
            )

        detail_text = result.get('detailDescription') or result.get('detailDescriptionText') or ''
        if isinstance(detail_text, str) and detail_text.strip():
            queries.append(
                f"MATCH (t:Table {{{table_props}}})\nSET t.detailDescription = '{escape_for_cypher(detail_text.strip())}'"
            )

        for column_info in result.get('columns', []) or []:
            column_name = (column_info.get('name') or '').strip()
            column_desc = (column_info.get('description') or '').strip()
            if not column_name or not column_desc:
                continue
            fqn = '.'.join(filter(None, [schema_prop, name_key, column_name])).lower()
            column_props = (
                f"user_id: '{self.user_id}', name: '{column_name}', fqn: '{fqn}', project_name: '{self.project_name}'"
            )
            queries.append(
                f"MATCH (c:Column {{{column_props}}})\nSET c.description = '{escape_for_cypher(column_desc)}'"
            )

        if queries:
            await self._send_queries(queries, self.file_last_line)

    def _build_table_merge(self, table_name: str, schema: Optional[str]) -> str:
        """테이블 MERGE 절을 생성합니다."""
        schema_value = schema or ''
        schema_part = f", schema: '{schema_value}'" if schema_value else ""
        return (
            f"MERGE (t:Table {{{self.table_base_props}, name: '{table_name}'{schema_part}, db: '{self.dbms}', project_name: '{self.project_name}'}})"
        )

    def _build_call_queries(self, node, analysis: Dict[str, Any]) -> List[str]:
        """요약 결과의 호출 정보를 CALL 관계 Cypher로 변환합니다."""
        queries: List[str] = []
        for call_name in analysis.get('calls', []) or []:
            if '.' in call_name:
                package_raw, proc_raw = call_name.split('.', 1)
                package_name = escape_for_cypher(package_raw.strip())
                proc_name = escape_for_cypher(proc_raw.strip())
                queries.append(
                    f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"OPTIONAL MATCH (p)\n"
                    f"WHERE (p:PROCEDURE OR p:FUNCTION)\n"
                    f"  AND p.folder_name = '{package_name}'\n"
                    f"  AND p.procedure_name = '{proc_name}'\n"
                    f"  AND p.user_id = '{self.user_id}'\n"
                    f"WITH c, p\n"
                    f"FOREACH(_ IN CASE WHEN p IS NULL THEN [1] ELSE [] END |\n"
                    f"    CREATE (new:PROCEDURE:FUNCTION {{folder_name: '{package_name}', procedure_name: '{proc_name}', user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
                    f"    MERGE (c)-[:CALL {{scope: 'external'}}]->(new))\n"
                    f"FOREACH(_ IN CASE WHEN p IS NOT NULL THEN [1] ELSE [] END |\n"
                    f"    MERGE (c)-[:CALL {{scope: 'external'}}]->(p))"
                )
            else:
                escaped_call = escape_for_cypher(call_name)
                queries.append(
                    f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"WITH c\n"
                    f"MATCH (p {{procedure_name: '{escaped_call}', {self.node_base_props}}})\n"
                    f"WHERE p:PROCEDURE OR p:FUNCTION\n"
                    f"MERGE (c)-[:CALL {{scope: 'internal'}}]->(p)"
                )
        return queries

    def _record_table_summary(self, schema: Optional[str], name: str, description: Optional[str]) -> Tuple[str, str]:
        """테이블 설명을 버킷에 누적해 후속 요약에 활용합니다."""
        schema_key = schema or ''
        name_key = name
        bucket = self._table_summary_store.get((schema_key, name_key))
        if bucket is None:
            bucket = {"summaries": set(), "columns": {}}
            self._table_summary_store[(schema_key, name_key)] = bucket
        text = (description or '').strip()
        if text:
            bucket["summaries"].add(text)
        return (schema_key, name_key)

    def _record_column_summary(
        self,
        table_key: Tuple[str, str],
        column_name: str,
        description: Optional[str],
        dtype: Optional[str] = None,
        nullable: Optional[bool] = None,
        examples: Optional[List[str]] = None,
    ):
        """컬럼 설명/메타를 버킷에 누적해 후속 요약에 활용합니다."""
        text = (description or '').strip()
        bucket = self._table_summary_store.setdefault(table_key, {"summaries": set(), "columns": {}})
        columns = bucket["columns"]
        canonical = column_name
        entry = columns.get(canonical)
        if entry is None:
            entry = {"name": column_name, "summaries": set(), "dtype": (dtype or ''), "nullable": True if nullable is None else bool(nullable), "examples": set()}
            columns[canonical] = entry
        if dtype is not None and not entry.get("dtype"):
            entry["dtype"] = dtype
        if nullable is not None:
            entry["nullable"] = bool(nullable)
        if text:
            entry["summaries"].add(text)
        if examples:
            for v in examples:
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    entry["examples"].add(s)

    async def _send_queries(self, queries: List[str], progress_line: int):
        """생성된 Cypher 쿼리를 순서대로 큐에 전송하고 완료 신호를 대기합니다."""
        if not queries:
            return
        await self.send_queue.put({
            "type": "analysis_code",
            "query_data": queries,
            "line_number": progress_line,
        })
        while True:
            response = await self.receive_queue.get()
            if response.get('type') == 'process_completed':
                break


class DbmsUnderstandingStrategy:
    """DBMS/SP 분석용 단일 전략 구현체.

    - statement_rules: AST 분류 규칙 제공
    - prepare_context: 실행 컨텍스트/LLM 호출기/적용기 준비
    - invoke_batch/apply_batch/finalize: 공통 루프가 호출하는 세 단계
    """

    def __init__(self):
        """전략 컨텍스트 준비 전까지 호출기/적용기를 None으로 둡니다."""
        self._invoker: Optional[DbmsUnderstandingInvoker] = None
        self._apply_manager: Optional[DbmsUnderstandingApplyManager] = None

    @property
    def name(self) -> str:
        return "dbms"

    def statement_rules(self) -> Dict[str, Any]:
        """AST 수집/분류에 필요한 DBMS 전용 구문 정의를 반환합니다."""
        return {
            "procedure_types": DBMS_PROCEDURE_TYPES,
            "non_analysis_types": DBMS_NON_ANALYSIS_TYPES,
            "non_next_recursive_types": DBMS_NON_NEXT_RECURSIVE_TYPES,
            "dml_statement_types": DBMS_DML_STATEMENT_TYPES,
            "variable_role_map": DBMS_VARIABLE_ROLE_MAP,
            "variable_declaration_types": DBMS_VARIABLE_DECLARATION_TYPES,
        }

    def prepare_context(
        self,
        *,
        node_base_props: str,
        folder_props: str,
        table_base_props: str,
        user_id: str,
        project_name: str,
        folder_name: str,
        file_name: str,
        dbms: str,
        api_key: str,
        locale: str,
        procedures: Dict[str, Any],
        send_queue: asyncio.Queue,
        receive_queue: asyncio.Queue,
        file_last_line: int,
    ) -> None:
        """분석 컨텍스트를 설정하고 내부 호출기/적용기를 준비합니다."""
        self._invoker = DbmsUnderstandingInvoker(api_key, locale)
        self._apply_manager = DbmsUnderstandingApplyManager(
            node_base_props=node_base_props,
            folder_props=folder_props,
            table_base_props=table_base_props,
            user_id=user_id,
            project_name=project_name,
            folder_name=folder_name,
            file_name=file_name,
            dbms=dbms,
            api_key=api_key,
            locale=locale,
            procedures=procedures,
            send_queue=send_queue,
            receive_queue=receive_queue,
            file_last_line=file_last_line,
        )

    def _ensure_ready(self):
        """prepare_context 이후에만 실행되도록 가드한다."""
        if not self._invoker or not self._apply_manager:
            raise RuntimeError("DbmsUnderstandingStrategy 컨텍스트가 설정되지 않았습니다. prepare_context를 먼저 호출하세요.")

    async def invoke_batch(self, batch):
        """배치 단위 LLM 호출을 실행한다."""
        self._ensure_ready()
        return await self._invoker.invoke(batch)

    async def apply_batch(self, batch, general, table):
        """LLM 결과를 Neo4j에 반영한다."""
        self._ensure_ready()
        await self._apply_manager.submit(batch, general, table)

    async def finalize(self):
        """남은 요약/테이블 후처리를 모두 끝낸다."""
        self._ensure_ready()
        await self._apply_manager.finalize()

    def build_call_queries(
        self,
        node,
        analysis: Dict[str, Any],
    ) -> List[str]:
        """요약 결과의 호출 정보를 CALL 관계 Cypher로 변환합니다."""
        self._ensure_ready()
        return self._apply_manager._build_call_queries(node, analysis)  # type: ignore[union-attr]

    async def process_variables(self, analyzer, nodes):
        """DBMS 변수 선언 처리를 수행합니다."""
        from understand.rules import understand_variables  # 지연 import

        targets = [node for node in nodes if node.node_type in analyzer.variable_declaration_types]
        if not targets:
            return

        proc_labels = sorted({node.procedure_name or "" for node in targets})
        if proc_labels:
            label_text = ', '.join(label for label in proc_labels if label) or '익명 프로시저'
            log_process("UNDERSTAND", "VAR", f"🔍 변수 선언 분석 시작: {label_text} ({analyzer.folder_file})")

        semaphore = asyncio.Semaphore(analyzer.variable_concurrency)

        async def build_variable_queries(node, analysis: Dict[str, Any]) -> List[str]:
            if not isinstance(analysis, dict):
                return []

            variables = analysis.get("variables") or []
            summary_payload = analysis.get("summary")
            summary_json = json.dumps(summary_payload if summary_payload is not None else "", ensure_ascii=False)

            role = analyzer.variable_role_map.get(node.node_type, "알 수 없는 매개변수")
            scope = "Global" if node.node_type == "PACKAGE_VARIABLE" else "Local"

            node_props = analyzer.node_base_props
            folder_props = analyzer.folder_props
            procedure_name = escape_for_cypher(node.procedure_name or '')

            if node.node_type == "PACKAGE_VARIABLE":
                node_match = f"startLine: {node.start_line}, {node_props}"
                base_var_props = f"{node_props}, role: '{role}', scope: '{scope}'"
            else:
                node_match = f"startLine: {node.start_line}, procedure_name: '{procedure_name}', {node_props}"
                base_var_props = f"{node_props}, procedure_name: '{procedure_name}', role: '{role}', scope: '{scope}'"

            queries: List[str] = []
            queries.append(
                f"MATCH (p:{node.node_type} {{{node_match}}})\nSET p.summary = {summary_json}"
            )

            for variable in variables:
                name_raw = (variable.get("name") or '').strip()
                if not name_raw:
                    continue

                name = escape_for_cypher(name_raw)
                var_type = escape_for_cypher(variable.get("type") or '')
                param_type = escape_for_cypher(variable.get("parameter_type") or '')
                value_json = json.dumps(variable.get("value") if variable.get("value") is not None else "", ensure_ascii=False)

                queries.append(
                    f"MERGE (v:Variable {{name: '{name}', {base_var_props}, type: '{var_type}', parameter_type: '{param_type}', value: {value_json}}})\n"
                    f"WITH v\n"
                    f"MATCH (p:{node.node_type} {{{node_match}}})\n"
                    f"MERGE (p)-[:SCOPE]->(v)\n"
                    f"WITH v\n"
                    f"MERGE (folder:SYSTEM {{{folder_props}}})\n"
                    f"MERGE (folder)-[:CONTAINS]->(v)"
                )

            return queries

        async def worker(node):
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        understand_variables,
                        node.get_raw_code(),
                        analyzer.api_key,
                        analyzer.locale,
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    log_process("UNDERSTAND", "VAR", f"❌ {node.node_type} ({node.start_line}~{node.end_line}) 변수 분석 중 오류 발생", logging.ERROR, exc)
                    return

                queries = await build_variable_queries(node, result)
                if queries:
                    await analyzer._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(node) for node in targets))
        if proc_labels:
            log_process("UNDERSTAND", "VAR", f"✅ 변수 선언 분석 완료: {label_text} ({analyzer.folder_file})")


