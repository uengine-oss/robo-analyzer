"""
클래스 다이어그램 생성 스크립트 (LLM 기반)

Neo4j에서 클래스/인터페이스 노드와 관계를 가져와서
LLM을 통해 순차적으로 Mermaid 클래스 다이어그램을 생성합니다.

토큰 초과를 방지하기 위해 배치 단위로 처리합니다.

사용법:
    python scripts/generate_class_diagram.py --project testjava --user TestSession
    python scripts/generate_class_diagram.py --project testjava --user TestSession --output diagram.md
    python scripts/generate_class_diagram.py --project testjava --user TestSession --no-llm
"""

import argparse
import os
import re
from typing import Any, Dict, List
from neo4j import GraphDatabase
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()


# ==================== 설정 ====================
MAX_CLASSES_PER_BATCH = 5  # 배치당 최대 클래스 수 (토큰 초과 방지)


# ==================== Neo4j 연결 ====================
class Neo4jConnection:
    """Neo4j 데이터베이스 연결 관리"""
    
    def __init__(self, database: str = None):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "password")
        self.database = database or os.getenv("TEST_DB_NAME", "neo4j")
        self.driver = None
    
    def connect(self):
        """Neo4j에 연결"""
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        print(f"✅ Neo4j 연결 성공: {self.uri} (DB: {self.database})")
    
    def close(self):
        """연결 종료"""
        if self.driver:
            self.driver.close()
            print("✅ Neo4j 연결 종료")
    
    def run_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        """쿼리 실행 및 결과 반환"""
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]


# ==================== LLM 클라이언트 ====================
class LLMClient:
    """LLM API 클라이언트"""
    
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")
        self.model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        
        if not self.api_key:
            raise ValueError("LLM_API_KEY 또는 OPENAI_API_KEY 환경 변수가 필요합니다")
    
    def call(self, system_prompt: str, user_prompt: str) -> str:
        """LLM API 호출"""
        import openai
        
        client = openai.OpenAI(api_key=self.api_key)
        
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1
        )
        
        return response.choices[0].message.content


# ==================== 데이터 조회 ====================
def fetch_classes(conn: Neo4jConnection, project: str, user: str) -> List[Dict]:
    """클래스/인터페이스 노드 조회 (필드, 메서드 포함)"""
    query = """
    MATCH (c)
    WHERE (c:CLASS OR c:INTERFACE)
      AND c.project_name = $project
      AND c.user_id = $user
      AND c.class_name IS NOT NULL
      AND c.startLine IS NOT NULL
    OPTIONAL MATCH (c)-[:PARENT_OF]->(f:FIELD)
    OPTIONAL MATCH (c)-[:PARENT_OF]->(m)
    WHERE (m:METHOD OR m:CONSTRUCTOR)
    WITH c, 
         collect(DISTINCT {
           name: f.name,
           field_type: COALESCE(f.field_type, ''),
           visibility: COALESCE(f.visibility, 'private'),
           target_class: f.target_class,
           node_code: f.node_code
         }) AS fields,
         collect(DISTINCT {
           name: COALESCE(m.methodName, ''),
           return_type: COALESCE(m.returnType, 'void'),
           visibility: COALESCE(m.visibility, 'public'),
           node_code: m.node_code
         }) AS methods
    RETURN c.class_name AS class_name,
           CASE WHEN 'INTERFACE' IN labels(c) AND NOT 'CLASS' IN labels(c) THEN 'interface' ELSE 'class' END AS class_type,
           c.summary AS summary,
           c.node_code AS class_code,
           fields,
           methods
    ORDER BY c.class_name
    """
    return conn.run_query(query, {"project": project, "user": user})


def fetch_relationships(conn: Neo4jConnection, project: str, user: str) -> List[Dict]:
    """관계 조회"""
    query = """
    MATCH (src)-[r]->(dst)
    WHERE (src:CLASS OR src:INTERFACE)
      AND (dst:CLASS OR dst:INTERFACE)
      AND src.project_name = $project
      AND src.user_id = $user
      AND src.class_name IS NOT NULL
      AND dst.class_name IS NOT NULL
      AND type(r) IN ['EXTENDS', 'IMPLEMENTS', 'ASSOCIATION', 'AGGREGATION', 'COMPOSITION', 'DEPENDENCY']
    RETURN DISTINCT
           src.class_name AS source,
           type(r) AS relationship,
           dst.class_name AS target,
           r.source_member AS source_member,
           r.multiplicity AS multiplicity
    ORDER BY src.class_name, type(r), dst.class_name
    """
    return conn.run_query(query, {"project": project, "user": user})


# ==================== 데이터 전처리 ====================
def extract_field_info_from_code(code: str) -> Dict[str, str]:
    """코드에서 필드 정보 추출"""
    if not code:
        return {}
    
    # Java 필드 패턴: (접근제어자) (static/final) 타입 이름 (= 값)?;
    pattern = r'(public|private|protected)?\s*(static)?\s*(final)?\s*(\w+(?:<[^>]+>)?)\s+(\w+)\s*(?:=|;)'
    match = re.search(pattern, code)
    
    if match:
        visibility = match.group(1) or "private"
        field_type = match.group(4) or ""
        field_name = match.group(5) or ""
        return {
            "name": field_name,
            "type": field_type,
            "visibility": visibility
        }
    return {}


def extract_method_info_from_code(code: str) -> Dict[str, str]:
    """코드에서 메서드 정보 추출"""
    if not code:
        return {}
    
    # Java 메서드 패턴: (접근제어자) (static)? (반환타입) 메서드명(
    pattern = r'(public|private|protected)?\s*(static)?\s*(\w+(?:<[^>]+>)?)\s+(\w+)\s*\('
    match = re.search(pattern, code)
    
    if match:
        visibility = match.group(1) or "public"
        return_type = match.group(3) or "void"
        method_name = match.group(4) or ""
        return {
            "name": method_name,
            "return_type": return_type,
            "visibility": visibility
        }
    return {}


def is_valid_field_name(name: str) -> bool:
    """유효한 필드명인지 확인 (FIELD[숫자] 형식 제외)"""
    if not name:
        return False
    if re.match(r'^FIELD\[\d+\]$', name):
        return False
    if re.match(r'^FIELD\d+$', name):
        return False
    return True


def preprocess_classes(classes: List[Dict]) -> List[Dict]:
    """클래스 데이터 전처리 - 필드/메서드 이름 정제"""
    processed = []
    
    for cls in classes:
        new_cls = {
            "class_name": cls["class_name"],
            "class_type": cls["class_type"],
            "summary": cls.get("summary"),
            "fields": [],
            "methods": []
        }
        
        # 필드 처리
        for field in (cls.get("fields") or []):
            if not field:
                continue
            
            name = field.get("name") or ""
            field_type = field.get("field_type") or field.get("type") or ""
            visibility = field.get("visibility") or "private"
            
            # FIELD[숫자] 형식이면 코드에서 추출 시도
            if not is_valid_field_name(name):
                extracted = extract_field_info_from_code(field.get("node_code") or "")
                if extracted.get("name"):
                    name = extracted["name"]
                    field_type = extracted.get("type") or field_type
                    visibility = extracted.get("visibility") or visibility
            
            # 여전히 유효하지 않으면 스킵
            if not is_valid_field_name(name):
                continue
            
            new_cls["fields"].append({
                "name": name,
                "type": field_type,
                "visibility": visibility
            })
        
        # 메서드 처리
        for method in (cls.get("methods") or []):
            if not method:
                continue
            
            name = method.get("name") or ""
            return_type = method.get("return_type") or "void"
            visibility = method.get("visibility") or "public"
            
            # 이름이 비어있으면 코드에서 추출 시도
            if not name:
                extracted = extract_method_info_from_code(method.get("node_code") or "")
                if extracted.get("name"):
                    name = extracted["name"]
                    return_type = extracted.get("return_type") or return_type
                    visibility = extracted.get("visibility") or visibility
            
            if not name:
                continue
            
            new_cls["methods"].append({
                "name": name,
                "return_type": return_type,
                "visibility": visibility
            })
        
        processed.append(new_cls)
    
    return processed


# ==================== 프롬프트 ====================
SYSTEM_PROMPT = """당신은 Mermaid 클래스 다이어그램 전문가입니다.
주어진 클래스 정보를 Mermaid classDiagram 문법으로 변환합니다.

## 필수 규칙

1. **클래스 정의 형식**:
```
class ClassName {
    -privateField type
    +publicMethod() returnType
}
```

2. **인터페이스 정의** - <<interface>>는 반드시 중괄호 안 첫 줄에:
```
class InterfaceName {
    <<interface>>
    +method() void
}
```

3. **접근 제어자**:
   - `+` : public
   - `-` : private
   - `#` : protected
   - `~` : default/package

4. **관계 화살표**:
   - 상속(EXTENDS): `Parent <|-- Child`
   - 구현(IMPLEMENTS): `Interface <|.. Class`
   - 연관(ASSOCIATION): `A --> B`
   - 집합(AGGREGATION): `A o-- B`
   - 합성(COMPOSITION): `A *-- B`
   - 의존(DEPENDENCY): `A ..> B`

## 출력 형식
- 코드만 출력 (설명, 마크다운 코드블록 없이)
- classDiagram 키워드로 시작하지 않음
- 들여쓰기 4칸"""


def create_class_prompt(classes: List[Dict]) -> str:
    """클래스 정의 생성 프롬프트"""
    prompt = "아래 클래스들을 Mermaid 클래스 정의로 변환하세요.\n\n"
    
    for cls in classes:
        class_type = "인터페이스" if cls["class_type"] == "interface" else "클래스"
        prompt += f"## {cls['class_name']} ({class_type})\n"
        
        fields = cls.get("fields") or []
        if fields:
            prompt += "필드:\n"
            for f in fields:
                prompt += f"  - [{f.get('visibility', 'private')}] {f.get('type', '')} {f['name']}\n"
        
        methods = cls.get("methods") or []
        if methods:
            prompt += "메서드:\n"
            for m in methods:
                prompt += f"  - [{m.get('visibility', 'public')}] {m['name']}() : {m.get('return_type', 'void')}\n"
        
        prompt += "\n"
    
    return prompt


def create_relationship_prompt(relationships: List[Dict]) -> str:
    """관계 정의 생성 프롬프트"""
    prompt = "아래 관계들을 Mermaid 관계 정의로 변환하세요.\n\n"
    
    for rel in relationships:
        rel_type_kr = {
            "EXTENDS": "상속",
            "IMPLEMENTS": "구현",
            "ASSOCIATION": "연관",
            "AGGREGATION": "집합",
            "COMPOSITION": "합성",
            "DEPENDENCY": "의존"
        }.get(rel["relationship"], rel["relationship"])
        
        prompt += f"- {rel['source']} --[{rel_type_kr}]--> {rel['target']}"
        if rel.get("source_member"):
            prompt += f" (필드: {rel['source_member']})"
        prompt += "\n"
    
    return prompt


# ==================== 다이어그램 생성 ====================
def generate_diagram_with_llm(
    llm: LLMClient, 
    classes: List[Dict], 
    relationships: List[Dict]
) -> str:
    """LLM을 사용하여 Mermaid 다이어그램 생성"""
    
    all_class_definitions = []
    all_relationships = []
    
    # 1. 클래스 정의 생성 (배치 처리)
    print(f"\n📦 클래스 정의 생성 중... (총 {len(classes)}개)")
    
    for i in range(0, len(classes), MAX_CLASSES_PER_BATCH):
        batch = classes[i:i + MAX_CLASSES_PER_BATCH]
        batch_num = i // MAX_CLASSES_PER_BATCH + 1
        total_batches = (len(classes) + MAX_CLASSES_PER_BATCH - 1) // MAX_CLASSES_PER_BATCH
        
        print(f"   배치 {batch_num}/{total_batches}: {[c['class_name'] for c in batch]}")
        
        prompt = create_class_prompt(batch)
        result = llm.call(SYSTEM_PROMPT, prompt)
        
        # 코드 블록 제거
        result = result.replace("```mermaid", "").replace("```", "")
        result = result.replace("classDiagram", "").strip()
        all_class_definitions.append(result)
    
    # 2. 관계 정의 생성
    if relationships:
        print(f"\n🔗 관계 정의 생성 중... (총 {len(relationships)}개)")
        
        rel_batch_size = 20
        for i in range(0, len(relationships), rel_batch_size):
            batch = relationships[i:i + rel_batch_size]
            
            prompt = create_relationship_prompt(batch)
            result = llm.call(SYSTEM_PROMPT, prompt)
            
            result = result.replace("```mermaid", "").replace("```", "")
            result = result.replace("classDiagram", "").strip()
            all_relationships.append(result)
    
    # 3. 최종 다이어그램 조합
    diagram_lines = ["```mermaid", "classDiagram"]
    
    # 클래스 정의 추가
    for class_def in all_class_definitions:
        for line in class_def.split("\n"):
            line = line.strip()
            if line:
                diagram_lines.append(f"    {line}")
    
    diagram_lines.append("")
    diagram_lines.append("    %% === 관계 ===")
    
    # 관계 추가
    for rel_def in all_relationships:
        for line in rel_def.split("\n"):
            line = line.strip()
            if line:
                diagram_lines.append(f"    {line}")
    
    diagram_lines.append("```")
    
    return "\n".join(diagram_lines)


def generate_diagram_simple(classes: List[Dict], relationships: List[Dict]) -> str:
    """LLM 없이 직접 Mermaid 다이어그램 생성"""
    lines = ["```mermaid", "classDiagram", ""]
    
    vis_map = {"public": "+", "private": "-", "protected": "#", "default": "~"}
    
    # 클래스 정의
    for cls in classes:
        name = cls["class_name"]
        class_type = cls["class_type"]
        fields = cls.get("fields") or []
        methods = cls.get("methods") or []
        
        lines.append(f"    class {name} {{")
        
        if class_type == "interface":
            lines.append("        <<interface>>")
        
        for field in fields:
            field_name = field.get("name") or ""
            if not field_name:
                continue
            vis = vis_map.get(field.get("visibility", "private"), "-")
            ftype = field.get("type") or ""
            if ftype:
                lines.append(f"        {vis}{ftype} {field_name}")
            else:
                lines.append(f"        {vis}{field_name}")
        
        for method in methods:
            method_name = method.get("name") or ""
            if not method_name:
                continue
            vis = vis_map.get(method.get("visibility", "public"), "+")
            rtype = method.get("return_type") or "void"
            lines.append(f"        {vis}{method_name}() {rtype}")
        
        lines.append("    }")
        lines.append("")
    
    # 관계
    lines.append("    %% === 관계 ===")
    
    arrow_map = {
        "EXTENDS": "<|--",
        "IMPLEMENTS": "<|..",
        "ASSOCIATION": "<--",
        "AGGREGATION": "o--",
        "COMPOSITION": "*--",
        "DEPENDENCY": "<..",
    }
    
    for rel in relationships:
        source = rel["source"]
        target = rel["target"]
        rel_type = rel["relationship"]
        source_member = rel.get("source_member") or ""
        
        arrow = arrow_map.get(rel_type, "<--")
        label = f" : {source_member}" if source_member else ""
        
        # 관계 방향: target <-- source (source가 target을 참조)
        lines.append(f"    {target} {arrow} {source}{label}")
    
    lines.append("```")
    
    return "\n".join(lines)


# ==================== 메인 ====================
def main():
    parser = argparse.ArgumentParser(description="Neo4j에서 Mermaid 클래스 다이어그램 생성")
    parser.add_argument("--project", required=True, help="프로젝트 이름")
    parser.add_argument("--user", required=True, help="사용자 ID")
    parser.add_argument("--database", help="Neo4j 데이터베이스 이름 (기본: TEST_DB_NAME 환경변수)")
    parser.add_argument("--output", help="출력 파일 경로")
    parser.add_argument("--no-llm", action="store_true", help="LLM 없이 직접 생성")
    
    args = parser.parse_args()
    
    # Neo4j 연결
    conn = Neo4jConnection(database=args.database)
    try:
        conn.connect()
        
        # 데이터 조회
        print(f"\n📊 프로젝트: {args.project}, 사용자: {args.user}")
        
        raw_classes = fetch_classes(conn, args.project, args.user)
        classes = preprocess_classes(raw_classes)
        
        print(f"✅ 클래스/인터페이스: {len(classes)}개")
        for cls in classes:
            field_count = len(cls.get("fields") or [])
            method_count = len(cls.get("methods") or [])
            print(f"   - {cls['class_name']} ({cls['class_type']}) [필드: {field_count}, 메서드: {method_count}]")
        
        relationships = fetch_relationships(conn, args.project, args.user)
        print(f"✅ 관계: {len(relationships)}개")
        
        # 관계 요약
        rel_summary = {}
        for rel in relationships:
            rel_type = rel["relationship"]
            rel_summary[rel_type] = rel_summary.get(rel_type, 0) + 1
        for rel_type, count in sorted(rel_summary.items()):
            print(f"   - {rel_type}: {count}개")
        
        if not classes:
            print("\n⚠️ 클래스가 없습니다.")
            return
        
        # 다이어그램 생성
        if args.no_llm:
            print("\n🔧 직접 다이어그램 생성 중...")
            diagram = generate_diagram_simple(classes, relationships)
        else:
            print("\n🤖 LLM으로 다이어그램 생성 중...")
            llm = LLMClient()
            diagram = generate_diagram_with_llm(llm, classes, relationships)
        
        # 출력
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(diagram)
            print(f"\n✅ 다이어그램 저장: {args.output}")
        else:
            print("\n" + "=" * 60)
            print(diagram)
            print("=" * 60)
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
