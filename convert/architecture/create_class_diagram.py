"""
클래스 다이어그램 생성 모듈
- Neo4j에서 클래스/관계 조회
- Mermaid classDiagram 문법으로 변환
"""

import json
import logging
from typing import List, Dict, Tuple, Any

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
            # 1. Neo4j에서 프로젝트 전체 클래스/인터페이스 및 관계 조회 (깊이 제한 없음)
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
        """클래스 그래프 조회 (프로젝트 전체, 깊이 제한 없음)"""

        rel_types = ", ".join([f"'{r}'" for r in CLASS_RELATION_TYPES])
        
        # 클래스 + 필드 + 메서드/생성자(+파라미터) 조회 (전체) - ENUM 포함
        class_query = f"""
        MATCH (c)
        WHERE (c:CLASS OR c:INTERFACE OR c:ENUM)
          AND c.project_name = '{self.project_name}'
          AND c.user_id = '{self.user_id}'

        WITH DISTINCT c AS cls
        
        // 1) 필드 조회 (cross product 방지를 위해 먼저 수집)
        OPTIONAL MATCH (cls)-[:PARENT_OF]->(f:FIELD)
        WITH cls, collect(DISTINCT {{
          name: f.name,
          type: COALESCE(f.field_type, f.type, ''),
          visibility: COALESCE(f.visibility, 'private')
        }}) AS fields

        // 2) 메서드/생성자 조회 + 파라미터 조회
        OPTIONAL MATCH (cls)-[:PARENT_OF]->(m)
        WHERE m:METHOD OR m:CONSTRUCTOR
        OPTIONAL MATCH (m)-[:HAS_PARAMETER]->(p:Parameter)
        WITH cls, fields, m, p
        ORDER BY p.index
        WITH cls, fields, m,
             collect(DISTINCT {{
               name: COALESCE(p.name, ''),
               type: COALESCE(p.type, ''),
               index: COALESCE(p.index, 0)
             }}) AS params
        WITH cls, fields,
             collect(DISTINCT {{
               name: COALESCE(m.name, ''),
               return_type: COALESCE(m.return_type, m.returnType, 'void'),
               visibility: COALESCE(m.visibility, 'public'),
               kind: CASE WHEN 'CONSTRUCTOR' IN labels(m) THEN 'constructor' ELSE 'method' END,
               parameters: params
             }}) AS methods
        
        RETURN 
            cls.directory AS directory,
            cls.class_name AS class_name,
            CASE 
              WHEN 'INTERFACE' IN labels(cls) THEN 'interface'
              WHEN 'ENUM' IN labels(cls) THEN 'enum'
              ELSE 'class'
            END AS class_type,
            COALESCE(cls.is_abstract, false) AS is_abstract,
            fields, methods
        ORDER BY cls.directory, cls.class_name
        """
        
        # 관계 조회 (전체) - ENUM 포함
        rel_query = f"""
        MATCH (src)-[r]->(dst)
        WHERE (src:CLASS OR src:INTERFACE OR src:ENUM)
          AND (dst:CLASS OR dst:INTERFACE OR dst:ENUM)
          AND src.project_name = '{self.project_name}'
          AND src.user_id = '{self.user_id}'
          AND dst.project_name = '{self.project_name}'
          AND dst.user_id = '{self.user_id}'
          AND type(r) IN [{rel_types}]
        
        RETURN DISTINCT
            src.directory AS src_directory,
            src.class_name AS source,
            type(r) AS relationship,
            dst.directory AS dst_directory,
            dst.class_name AS target,
            r.source_members AS label
        ORDER BY src.directory, src.class_name
        """
        
        results = await conn.execute_queries([class_query, rel_query])
        
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
                "directory": cls.get("directory", ""),
                "class_name": cls["class_name"],
                "class_type": cls.get("class_type", "class"),
                "is_abstract": cls.get("is_abstract", False),
                "fields": fields,
                "methods": methods
            })
        return result

    @staticmethod
    def _format_params(params: List[Dict[str, Any]]) -> str:
        """Mermaid 메서드 파라미터 문자열 생성: 'Type name, Type2 name2'"""
        if not params:
            return ""
        parts: List[str] = []
        for p in params:
            if not p:
                continue
            ptype = (p.get("type") or "").strip()
            pname = (p.get("name") or "").strip()
            if ptype and pname:
                parts.append(f"{ptype} {pname}")
            elif pname:
                parts.append(pname)
            elif ptype:
                parts.append(ptype)
        return ", ".join(parts)

    @staticmethod
    def _normalize_rel_type(rel_type: str) -> str:
        """관계 타입 정규화."""
        return (rel_type or "ASSOCIATION").strip().upper()

    def _dedupe_relationships(self, relationships: List[Dict]) -> List[Dict]:
        """관계 중복/과다를 정리합니다.
        
        - 동일 (source, target, label) 조합에서 ASSOCIATION/AGGREGATION/COMPOSITION은 '더 강한' 타입 1개만 남김
        - 완전 중복은 제거
        """
        if not relationships:
            return []

        strength = {"COMPOSITION": 3, "AGGREGATION": 2, "ASSOCIATION": 1}
        picked: Dict[Tuple[str, str, str], Dict] = {}
        passthrough: List[Dict] = []

        for rel in relationships:
            src = rel.get("source", "") or ""
            dst = rel.get("target", "") or ""
            rel_type = self._normalize_rel_type(rel.get("relationship"))
            label_raw = rel.get("label", "") or ""
            # label은 리스트(예: source_members)로 올 수 있음 → 문자열 키로 정규화
            if isinstance(label_raw, list):
                label = ", ".join([str(x) for x in label_raw if x])
            else:
                label = str(label_raw or "")

            # 상속/구현/의존은 그대로 유지 (중복만 제거)
            if rel_type not in strength:
                key = (src, dst, rel_type, label)
                if key in picked:
                    continue
                picked[key] = rel
                passthrough.append(rel)
                continue

            key2 = (src, dst, label)
            prev = picked.get(key2)
            if not prev:
                picked[key2] = rel
                continue
            prev_type = self._normalize_rel_type(prev.get("relationship"))
            if strength.get(rel_type, 0) > strength.get(prev_type, 0):
                picked[key2] = rel

        # picked 안에는 혼재(2종 키)하므로, association류만 따로 추출
        assoc_like: List[Dict] = []
        for k, v in picked.items():
            if isinstance(k, tuple) and len(k) == 3:
                assoc_like.append(v)

        return passthrough + assoc_like
    
    async def _generate_diagram(
        self,
        classes: List[Dict],
        relationships: List[Dict]
    ) -> str:
        """Mermaid 다이어그램 생성 (LLM 없이 직접 생성)"""
        return self._build_diagram_direct(classes, relationships)
    
    def _build_diagram_direct(self, classes: List[Dict], relationships: List[Dict]) -> str:
        """직접 Mermaid 다이어그램 빌드"""
        lines = ["```mermaid", "classDiagram", ""]
        
        # 클래스 정의
        for cls in classes:
            name = cls["class_name"]
            class_type = cls.get("class_type", "class")
            is_abstract = cls.get("is_abstract", False)
            
            # Mermaid 예약어 충돌 방지: 클래스 이름을 백틱으로 감쌈
            lines.append(f"    class `{name}` {{")
            
            # 클래스 타입 스테레오타입 추가
            if class_type == "interface":
                lines.append("        <<interface>>")
            elif class_type == "enum":
                lines.append("        <<enumeration>>")
            
            # abstract 클래스 표시
            if is_abstract and class_type == "class":
                lines.append("        <<abstract>>")
            
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
                params_str = self._format_params(m.get("parameters") or [])
                kind = (m.get("kind") or "method").strip().lower()
                if kind == "constructor":
                    lines.append(f"        {vis}{mname}({params_str})")
                else:
                    lines.append(f"        {vis}{mname}({params_str}) {rtype}")
            
            lines.append("    }")
            lines.append("")
        
        # 관계
        relationships = self._dedupe_relationships(relationships or [])
        if relationships:
            lines.append("    %% Relationships")
            for rel in relationships:
                src = rel.get("source", "")
                dst = rel.get("target", "")
                rel_type = rel.get("relationship", "ASSOCIATION")
                label = rel.get("label", "")
                arrow = ARROW_MAP.get(rel_type, "-->")
                if isinstance(label, list):
                    label = ", ".join([str(x) for x in label if x])
                label_str = f" : {label}" if (label or "").strip() else ""
                # 관계에서도 클래스 이름을 백틱으로 감쌈
                lines.append(f"    `{src}` {arrow} `{dst}`{label_str}")
        
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

