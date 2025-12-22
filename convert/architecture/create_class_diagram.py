"""
클래스 다이어그램 생성 모듈
- Neo4j에서 클래스/관계 조회
- Mermaid classDiagram 문법으로 변환
"""

import logging
from typing import List, Dict, Tuple, Any

from understand.neo4j_connection import Neo4jConnection


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
    
    def __init__(self, project_name: str, user_id: str):
        self.project_name = project_name
        self.user_id = user_id
    
    async def generate(self, directories: List[Tuple[str, str]]) -> Dict:
        """
        클래스 다이어그램 생성
        
        Args:
            directories: [("directory/file.java", "ClassName"), ...] (directory, class_name) 튜플 리스트.
                        현재 구현에서는 프로젝트 전체 클래스를 대상으로 다이어그램을 생성하며,
                        전달된 값은 필터링에 사용되지 않습니다. (레거시 동작과 동일)
        
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
            classes, relationships = await self._fetch_class_graph(connection)
            
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
    
    def _build_base_conditions(self, node_alias: str = "c") -> str:
        """공통 WHERE 조건 문자열 생성"""
        return f"{node_alias}.project_name = '{self.project_name}' AND {node_alias}.user_id = '{self.user_id}'"
    
    def _build_class_query(self) -> str:
        """클래스 + 필드 + 메서드 조회 쿼리 생성 (프로젝트 전체 대상, 레거시 동작과 동일)"""
        base_conditions = self._build_base_conditions("c")
        
        return f"""
        MATCH (c)
        WHERE (c:CLASS OR c:INTERFACE OR c:ENUM)
          AND {base_conditions}

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
    
    def _build_relationship_query(self) -> str:
        """관계 조회 쿼리 생성 (is_value_object 필터링 포함, 프로젝트 전체 대상)"""
        src_conditions = self._build_base_conditions("src")
        dst_conditions = self._build_base_conditions("dst")
        rel_types = ", ".join([f"'{r}'" for r in CLASS_RELATION_TYPES])
        
        return f"""
        MATCH (src)-[r]->(dst)
        WHERE (src:CLASS OR src:INTERFACE OR src:ENUM)
          AND (dst:CLASS OR dst:INTERFACE OR dst:ENUM)
          AND {src_conditions}
          AND {dst_conditions}
          AND type(r) IN [{rel_types}]
          AND (type(r) <> 'DEPENDENCY' OR r.is_value_object IS NULL OR r.is_value_object = false)
        
        RETURN DISTINCT
            src.directory AS src_directory,
            src.class_name AS source,
            type(r) AS relationship,
            dst.directory AS dst_directory,
            dst.class_name AS target,
            r.source_members AS label,
            r.multiplicity AS multiplicity
        ORDER BY src.directory, src.class_name
        """
    
    def _build_inheritance_query(self) -> str:
        """상속 관계 조회 쿼리 생성 (프로젝트 전체 대상)"""
        child_conditions = self._build_base_conditions("child")
        parent_conditions = self._build_base_conditions("parent")
        
        return f"""
        MATCH (child)-[:EXTENDS|IMPLEMENTS*]->(parent)
        WHERE (child:CLASS OR child:INTERFACE OR child:ENUM)
          AND (parent:CLASS OR parent:INTERFACE OR parent:ENUM)
          AND {child_conditions}
          AND {parent_conditions}
        
        RETURN DISTINCT
            child.class_name AS child,
            parent.class_name AS parent
        """
    
    async def _fetch_class_graph(
        self,
        conn: Neo4jConnection,
    ) -> Tuple[List[Dict], List[Dict]]:
        """클래스 그래프 조회 (프로젝트 전체, 깊이 제한 없음)"""
        class_query = self._build_class_query()
        rel_query = self._build_relationship_query()
        inheritance_query = self._build_inheritance_query()
        
        results = await conn.execute_queries([class_query, rel_query, inheritance_query])
        
        classes = self._filter_classes(results[0] if results else [])
        relationships = results[1] if len(results) > 1 else []
        inheritance_map = self._build_inheritance_map(results[2] if len(results) > 2 else [])
        
        # 노이즈 DEPENDENCY 제거 및 중복 제거
        relationships = self._filter_noise_dependencies(relationships, inheritance_map)
        relationships = self._dedupe_relationships(relationships)
        
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

    def _build_inheritance_map(self, inheritance_results: List[Dict]) -> Dict[str, List[str]]:
        """상속 관계 맵 생성: {child: [parent1, parent2, ...]}"""
        inheritance_map: Dict[str, List[str]] = {}
        for row in inheritance_results:
            child = row.get("child", "")
            parent = row.get("parent", "")
            if child and parent:
                if child not in inheritance_map:
                    inheritance_map[child] = []
                inheritance_map[child].append(parent)
        return inheritance_map
    
    def _get_all_ancestors(self, class_name: str, inheritance_map: Dict[str, List[str]]) -> set:
        """클래스의 모든 조상 클래스 반환 (재귀적)"""
        ancestors = set()
        if class_name not in inheritance_map:
            return ancestors
        
        for parent in inheritance_map[class_name]:
            ancestors.add(parent)
            ancestors.update(self._get_all_ancestors(parent, inheritance_map))
        
        return ancestors
    
    def _build_ownership_map(self, relationships: List[Dict]) -> Dict[str, Dict[str, str]]:
        """소유 관계 맵 생성: {source: {target: relationship_type}}"""
        ownership_map: Dict[str, Dict[str, str]] = {}
        ownership_types = {"COMPOSITION", "AGGREGATION", "ASSOCIATION"}
        
        for rel in relationships:
            src = rel.get("source", "") or ""
            dst = rel.get("target", "") or ""
            rel_type = self._normalize_rel_type(rel.get("relationship"))
            
            if rel_type in ownership_types:
                if src not in ownership_map:
                    ownership_map[src] = {}
                ownership_map[src][dst] = rel_type
        
        return ownership_map
    
    def _is_noise_dependency(
        self,
        src: str,
        dst: str,
        inheritance_map: Dict[str, List[str]],
        ownership_map: Dict[str, Dict[str, str]]
    ) -> bool:
        """노이즈 DEPENDENCY인지 판단
        
        상위 클래스에서 이미 소유 관계가 있는 타겟으로의 DEPENDENCY는 노이즈입니다.
        """
        ancestors = self._get_all_ancestors(src, inheritance_map)
        for ancestor in ancestors:
            if ancestor in ownership_map and dst in ownership_map[ancestor]:
                return True
        return False
    
    def _filter_noise_dependencies(
        self, 
        relationships: List[Dict], 
        inheritance_map: Dict[str, List[str]]
    ) -> List[Dict]:
        """상속 체인을 통한 노이즈 DEPENDENCY 제거
        
        상위 클래스에서 이미 COMPOSITION/AGGREGATION/ASSOCIATION 관계가 있는 타겟으로의 
        DEPENDENCY를 하위 클래스에서 제거합니다.
        """
        if not relationships:
            return []
        
        ownership_map = self._build_ownership_map(relationships)
        filtered_rels = []
        
        for rel in relationships:
            src = rel.get("source", "") or ""
            dst = rel.get("target", "") or ""
            rel_type = self._normalize_rel_type(rel.get("relationship"))
            
            # DEPENDENCY 관계만 필터링
            if rel_type == "DEPENDENCY":
                if not self._is_noise_dependency(src, dst, inheritance_map, ownership_map):
                    filtered_rels.append(rel)
            else:
                filtered_rels.append(rel)
        
        return filtered_rels
    
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
        """Mermaid 다이어그램 생성"""
        return self._build_diagram_direct(classes, relationships)
    
    def _format_class_definition(self, cls: Dict) -> List[str]:
        """클래스 정의를 Mermaid 형식으로 변환"""
        lines = []
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
            if ftype:
                lines.append(f"        {vis}{ftype} {fname}")
            else:
                lines.append(f"        {vis}{fname}")
        
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
        return lines
    
    def _format_relationship(self, rel: Dict) -> str:
        """관계를 Mermaid 형식으로 변환"""
        src = rel.get("source", "")
        dst = rel.get("target", "")
        rel_type = rel.get("relationship", "ASSOCIATION")
        label = rel.get("label", "")
        
        arrow = ARROW_MAP.get(rel_type, "-->")
        
        # label 정규화
        if isinstance(label, list):
            label = ", ".join([str(x) for x in label if x])
        else:
            label = str(label or "")
        
        label_str = f" : {label}" if label.strip() else ""
        return f"    `{src}` {arrow} `{dst}`{label_str}"
    
    def _build_diagram_direct(self, classes: List[Dict], relationships: List[Dict]) -> str:
        """직접 Mermaid 다이어그램 빌드"""
        lines = ["```mermaid", "classDiagram", ""]
        
        # 클래스 정의
        for cls in classes:
            lines.extend(self._format_class_definition(cls))
            lines.append("")
        
        # 관계
        if relationships:
            lines.append("    %% Relationships")
            for rel in relationships:
                lines.append(self._format_relationship(rel))
        
        lines.append("```")
        return "\n".join(lines)


async def start_class_diagram_generation(
    directories: List[Tuple[str, str]],
    project_name: str,
    user_id: str
) -> Dict:
    """
    클래스 다이어그램 생성 시작점
    
    Args:
        directories: [("dir/file.java", "ClassName"), ...] (directory, class_name) 튜플 리스트
        project_name: 프로젝트 이름
        user_id: 사용자 ID
    
    Returns:
        dict: 다이어그램 결과
    """
    generator = ClassDiagramGenerator(project_name, user_id)
    return await generator.generate(directories)

