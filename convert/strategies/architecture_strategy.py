"""
Architecture 변환 전략
- Framework understanding 결과를 기반으로 Mermaid 클래스 다이어그램 생성
- 형식: "systemName/className" → 해당 클래스 + 1단계 연결 클래스 + 필드/메서드
"""

import json
import logging
from typing import AsyncGenerator, Any, List, Dict, Tuple

from .base_strategy import ConversionStrategy
from understand.neo4j_connection import Neo4jConnection
from util.rule_loader import RuleLoader
from util.utility_tool import emit_message, emit_data, emit_error


logger = logging.getLogger(__name__)


class ArchitectureConversionStrategy(ConversionStrategy):
    """Architecture 변환 전략 (Mermaid 클래스 다이어그램 생성)"""
    
    # Mermaid 접근제어자 매핑
    VISIBILITY_MAP = {"public": "+", "private": "-", "protected": "#", "default": "~"}
    
    # Mermaid 관계 화살표 매핑
    ARROW_MAP = {
        "EXTENDS": "<|--",
        "IMPLEMENTS": "<|..",
        "ASSOCIATION": "-->",
        "AGGREGATION": "o--",
        "COMPOSITION": "*--",
        "DEPENDENCY": "..>",
    }
    
    # 클래스 관계 타입
    CLASS_RELATION_TYPES = ['EXTENDS', 'IMPLEMENTS', 'ASSOCIATION', 'AGGREGATION', 'COMPOSITION', 'DEPENDENCY']
    
    def __init__(self, target: str = "mermaid"):
        self.target = target.lower()
        self.rule_loader = RuleLoader(target_lang="mermaid", domain="convert")
    
    async def convert(
        self,
        file_names: list,
        orchestrator: Any,
        class_names: List[Tuple[str, str]] = None,
        **kwargs
    ) -> AsyncGenerator[bytes, None]:
        """
        클래스 다이어그램 변환 수행
        
        Args:
            file_names: 사용 안함
            orchestrator: ServiceOrchestrator
            class_names: [(systemName, className), ...] 튜플 리스트
        """
        if not class_names:
            yield emit_error("class_names가 필요합니다. 형식: [(systemName, className), ...]")
            return
        
        connection = Neo4jConnection()
        
        try:
            yield emit_message(f"클래스 다이어그램 생성 시작: {len(class_names)}개 클래스")
            
            # 1. 클래스 + 1단계 연결 클래스 조회
            classes, relationships = await self._fetch_class_graph(
                connection, orchestrator.user_id, orchestrator.project_name, class_names
            )
            
            if not classes:
                yield emit_error("선택한 클래스를 찾을 수 없습니다.")
                return
            
            yield emit_message(f"조회 완료: {len(classes)}개 클래스, {len(relationships)}개 관계")
            
            # 2. Mermaid 다이어그램 생성
            diagram = await self._generate_diagram(classes, relationships, orchestrator.api_key, orchestrator.locale)
            
            # 3. 결과 반환
            yield emit_data(
                file_type="mermaid_diagram",
                diagram=diagram,
                class_count=len(classes),
                relationship_count=len(relationships)
            )
            
            yield emit_message("클래스 다이어그램 생성 완료")
            
        except Exception as e:
            logger.error(f"Architecture 변환 오류: {e}")
            yield emit_error(f"다이어그램 생성 실패: {str(e)}")
        finally:
            await connection.close()
    
    async def _fetch_class_graph(
        self,
        conn: Neo4jConnection,
        user_id: str,
        project_name: str,
        class_names: List[Tuple[str, str]]
    ) -> Tuple[List[Dict], List[Dict]]:
        """클래스 그래프 조회 (선택 클래스 + 1단계 연결 클래스)"""
        
        # WHERE 조건 생성: (folder_name = 'sys1' AND class_name = 'Class1') OR ...
        conditions = " OR ".join([
            f"(c.folder_name = '{sys}' AND c.class_name = '{cls}')"
            for sys, cls in class_names
        ])
        rel_types = ", ".join([f"'{r}'" for r in self.CLASS_RELATION_TYPES])
        
        # 클래스 + 필드 + 메서드 조회
        class_query = f"""
        MATCH (c)
        WHERE (c:CLASS OR c:INTERFACE)
          AND c.project_name = $project_name
          AND c.user_id = $user_id
          AND ({conditions})
        
        // 1단계 연결 클래스
        OPTIONAL MATCH (c)-[r1]-(related)
        WHERE (related:CLASS OR related:INTERFACE)
          AND related.project_name = $project_name
          AND related.user_id = $user_id
          AND type(r1) IN [{rel_types}]
        
        WITH collect(DISTINCT c) + collect(DISTINCT related) AS all_classes
        UNWIND all_classes AS cls
        WITH DISTINCT cls WHERE cls IS NOT NULL
        
        // 필드, 메서드 조회
        OPTIONAL MATCH (cls)-[:PARENT_OF]->(f:FIELD)
        OPTIONAL MATCH (cls)-[:PARENT_OF]->(m)
        WHERE m:METHOD OR m:CONSTRUCTOR
        
        WITH cls,
             collect(DISTINCT {{
               name: f.name,
               type: COALESCE(f.field_type, f.type, ''),
               visibility: COALESCE(f.visibility, 'private')
             }}) AS fields,
             collect(DISTINCT {{
               name: COALESCE(m.methodName, m.name, ''),
               return_type: COALESCE(m.returnType, 'void'),
               visibility: COALESCE(m.visibility, 'public')
             }}) AS methods
        
        RETURN 
            cls.folder_name AS system_name,
            cls.class_name AS class_name,
            CASE WHEN 'INTERFACE' IN labels(cls) THEN 'interface' ELSE 'class' END AS class_type,
            fields, methods
        ORDER BY cls.folder_name, cls.class_name
        """
        
        # 관계 조회
        rel_query = f"""
        MATCH (c)
        WHERE (c:CLASS OR c:INTERFACE)
          AND c.project_name = $project_name
          AND c.user_id = $user_id
          AND ({conditions})
        
        OPTIONAL MATCH (c)-[r1]-(related)
        WHERE (related:CLASS OR related:INTERFACE)
          AND related.project_name = $project_name
          AND type(r1) IN [{rel_types}]
        
        WITH collect(DISTINCT c) + collect(DISTINCT related) AS all_nodes
        
        MATCH (src)-[r]->(dst)
        WHERE src IN all_nodes AND dst IN all_nodes
          AND type(r) IN [{rel_types}]
        
        RETURN DISTINCT
            src.folder_name AS src_system,
            src.class_name AS source,
            type(r) AS relationship,
            dst.folder_name AS dst_system,
            dst.class_name AS target,
            r.source_member AS label
        ORDER BY src.folder_name, src.class_name
        """
        
        params = {"project_name": project_name, "user_id": user_id}
        results = await conn.execute_queries([class_query, rel_query], params)
        
        classes = self._filter_classes(results[0] if results else [])
        relationships = results[1] if len(results) > 1 else []
        
        return classes, relationships
    
    def _filter_classes(self, raw_classes: List[Dict]) -> List[Dict]:
        """클래스 데이터 정제 (빈 값 필터링)"""
        result = []
        for cls in raw_classes:
            if not cls.get("class_name"):
                continue
            
            fields = [f for f in (cls.get("fields") or []) if f and f.get("name")]
            methods = [m for m in (cls.get("methods") or []) if m and m.get("name")]
            
            result.append({
                "system_name": cls.get("system_name", ""),
                "class_name": cls["class_name"],
                "class_type": cls.get("class_type", "class"),
                "fields": fields,
                "methods": methods
            })
        return result
    
    async def _generate_diagram(
        self,
        classes: List[Dict],
        relationships: List[Dict],
        api_key: str,
        locale: str
    ) -> str:
        """Mermaid 다이어그램 생성"""
        
        # 클래스 5개 이하면 직접 생성 (LLM 호출 불필요)
        if len(classes) <= 5:
            return self._build_diagram_direct(classes, relationships)
        
        # LLM으로 생성
        inputs = {
            "classes": json.dumps(classes, ensure_ascii=False, indent=2),
            "relationships": json.dumps(relationships, ensure_ascii=False, indent=2),
            "locale": locale
        }
        
        result = self.rule_loader.execute(role_name="diagram", inputs=inputs, api_key=api_key)
        diagram = result.get("diagram", "")
        
        if not diagram.startswith("```"):
            diagram = f"```mermaid\nclassDiagram\n{diagram}\n```"
        
        return diagram
    
    def _build_diagram_direct(self, classes: List[Dict], relationships: List[Dict]) -> str:
        """직접 Mermaid 다이어그램 빌드 (소규모용)"""
        lines = ["```mermaid", "classDiagram", ""]
        
        # 클래스 정의
        for cls in classes:
            name = cls["class_name"]
            lines.append(f"    class {name} {{")
            
            if cls["class_type"] == "interface":
                lines.append("        <<interface>>")
            
            # 필드
            for f in cls.get("fields", []):
                vis = self.VISIBILITY_MAP.get(f.get("visibility", "private"), "-")
                ftype = f.get("type", "")
                fname = f.get("name", "")
                lines.append(f"        {vis}{ftype} {fname}" if ftype else f"        {vis}{fname}")
            
            # 메서드
            for m in cls.get("methods", []):
                vis = self.VISIBILITY_MAP.get(m.get("visibility", "public"), "+")
                mname = m.get("name", "")
                rtype = m.get("return_type", "void")
                lines.append(f"        {vis}{mname}() {rtype}")
            
            lines.append("    }")
            lines.append("")
        
        # 관계
        if relationships:
            lines.append("    %% Relationships")
            for rel in relationships:
                src = rel.get("source", "")
                dst = rel.get("target", "")
                rel_type = rel.get("relationship", "ASSOCIATION")
                label = rel.get("label", "")
                arrow = self.ARROW_MAP.get(rel_type, "-->")
                label_str = f" : {label}" if label else ""
                lines.append(f"    {dst} {arrow} {src}{label_str}")
        
        lines.append("```")
        return "\n".join(lines)
