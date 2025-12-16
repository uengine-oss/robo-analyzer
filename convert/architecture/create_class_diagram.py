"""
클래스 다이어그램 생성 모듈
- Neo4j에서 클래스/관계 조회
- Mermaid classDiagram 문법으로 변환
"""

import json
import logging
from typing import List, Dict, Tuple

from understand.neo4j_connection import Neo4jConnection
from util.rule_loader import RuleLoader


logger = logging.getLogger(__name__)


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


class ClassDiagramGenerator:
    """Mermaid 클래스 다이어그램 생성기"""
    
    def __init__(self, project_name: str, user_id: str, api_key: str, locale: str):
        self.project_name = project_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.rule_loader = RuleLoader(target_lang="mermaid", domain="convert")
    
    async def generate(self, class_names: List[Tuple[str, str]]) -> Dict:
        """
        클래스 다이어그램 생성
        
        Args:
            class_names: [(systemName, className), ...] 튜플 리스트
        
        Returns:
            dict: {
                "diagram": "```mermaid\n...",
                "class_count": int,
                "relationship_count": int,
                "classes": [...],
                "relationships": [...]
            }
        """
        connection = Neo4jConnection()
        
        try:
            # 1. Neo4j에서 클래스 + 1단계 연결 클래스 조회
            classes, relationships = await self._fetch_class_graph(connection, class_names)
            
            if not classes:
                raise ValueError("선택한 클래스를 찾을 수 없습니다.")
            
            # 2. Mermaid 다이어그램 생성
            diagram = await self._generate_diagram(classes, relationships)
            
            return {
                "diagram": diagram,
                "class_count": len(classes),
                "relationship_count": len(relationships),
                "classes": classes,
                "relationships": relationships
            }
            
        finally:
            await connection.close()
    
    async def _fetch_class_graph(
        self,
        conn: Neo4jConnection,
        class_names: List[Tuple[str, str]]
    ) -> Tuple[List[Dict], List[Dict]]:
        """클래스 그래프 조회 (선택 클래스 + 1단계 연결 클래스)"""
        
        # WHERE 조건 생성: (system_name = 'sys1' AND class_name = 'Class1') OR ...
        conditions = " OR ".join([
            f"(c.system_name = '{sys}' AND c.class_name = '{cls}')"
            for sys, cls in class_names
        ])
        rel_types = ", ".join([f"'{r}'" for r in CLASS_RELATION_TYPES])
        
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
            cls.system_name AS system_name,
            cls.class_name AS class_name,
            CASE WHEN 'INTERFACE' IN labels(cls) THEN 'interface' ELSE 'class' END AS class_type,
            fields, methods
        ORDER BY cls.system_name, cls.class_name
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
            src.system_name AS src_system,
            src.class_name AS source,
            type(r) AS relationship,
            dst.system_name AS dst_system,
            dst.class_name AS target,
            r.source_member AS label
        ORDER BY src.system_name, src.class_name
        """
        
        params = {"project_name": self.project_name, "user_id": self.user_id}
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
        relationships: List[Dict]
    ) -> str:
        """Mermaid 다이어그램 생성"""
        
        # 클래스 5개 이하면 직접 생성 (LLM 호출 불필요)
        if len(classes) <= 5:
            return self._build_diagram_direct(classes, relationships)
        
        # LLM으로 생성
        inputs = {
            "classes": json.dumps(classes, ensure_ascii=False, indent=2),
            "relationships": json.dumps(relationships, ensure_ascii=False, indent=2),
            "locale": self.locale
        }
        
        result = self.rule_loader.execute(role_name="diagram", inputs=inputs, api_key=self.api_key)
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
                vis = VISIBILITY_MAP.get(f.get("visibility", "private"), "-")
                ftype = f.get("type", "")
                fname = f.get("name", "")
                lines.append(f"        {vis}{ftype} {fname}" if ftype else f"        {vis}{fname}")
            
            # 메서드
            for m in cls.get("methods", []):
                vis = VISIBILITY_MAP.get(m.get("visibility", "public"), "+")
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
                arrow = ARROW_MAP.get(rel_type, "-->")
                label_str = f" : {label}" if label else ""
                lines.append(f"    {dst} {arrow} {src}{label_str}")
        
        lines.append("```")
        return "\n".join(lines)


async def start_class_diagram_generation(
    class_names: List[Tuple[str, str]],
    project_name: str,
    user_id: str,
    api_key: str,
    locale: str
) -> Dict:
    """
    클래스 다이어그램 생성 시작점
    
    Args:
        class_names: [(systemName, className), ...] 튜플 리스트
        project_name: 프로젝트 이름
        user_id: 사용자 ID
        api_key: LLM API 키
        locale: 로케일
    
    Returns:
        dict: 다이어그램 결과
    """
    generator = ClassDiagramGenerator(project_name, user_id, api_key, locale)
    return await generator.generate(class_names)

