"""
코드 병합 로직 (변환 로직과 동일한 방식)
- 시작 라인 순서대로 순차 병합
- 부모-자식 관계를 스택으로 관리
- 범위를 벗어나면 자식들을 부모에 치환
"""

import logging
import textwrap
from typing import Dict, Any, List, Optional
from understand.neo4j_connection import Neo4jConnection
from util.utility_tool import escape_for_cypher

logger = logging.getLogger(__name__)
CODE_PLACEHOLDER = "...code..."  # 변환 로직과 동일


async def merge_fixed_code(
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str,
    conversion_type: str,
    target: str,
    skeleton_code: str
) -> str:
    """
    수정된 CONVERSION_BLOCK들을 기반으로 최종 코드를 병합합니다.
    
    Args:
        skeleton_code: 스켈레톤 코드 (CodePlaceHolder 포함)
        
    Returns:
        병합된 최종 코드
    """
    connection = Neo4jConnection()
    try:
        project_condition = f", project_name: '{escape_for_cypher(project_name)}'" if project_name else ""
        
        # 모든 블록을 시작 라인 순서대로 가져오기 (부모-자식 관계 포함)
        query = f"""
            MATCH (conv:CONVERTING {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}'{project_condition},
                conversion_type: '{escape_for_cypher(conversion_type)}',
                target: '{escape_for_cypher(target)}'
            }})-[:HAS_BLOCK]->(block:CONVERSION_BLOCK {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}'{project_condition}
            }})
            OPTIONAL MATCH (block)-[:PARENT_OF]->(child:CONVERSION_BLOCK {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}'{project_condition}
            }})
            WITH block, count(child) as child_count
            WITH block, (child_count > 0) as has_children
            ORDER BY block.start_line
            RETURN block, has_children
        """
        
        results = await connection.execute_queries([query])
        
        if not results or len(results) == 0 or len(results[0]) == 0:
            logger.warning("병합할 블록을 찾지 못했습니다.")
            return skeleton_code.replace("CodePlaceHolder", "")
        
        # 모든 블록을 시작 라인 순서대로 가져오기
        all_blocks = []
        for row in results[0]:
            block = dict(row.get('block'))
            has_children = row.get('has_children', False)
            if block:
                block['has_children'] = has_children
                all_blocks.append(block)
        
        # 변환 로직과 동일한 방식으로 병합
        merged_code = await _merge_blocks_sequentially(
            all_blocks,
            folder_name,
            file_name,
            procedure_name,
            user_id,
            project_name
        )
        
        # 스켈레톤과 병합
        final_code = skeleton_code.replace("CodePlaceHolder", merged_code.strip())
        
        logger.info("✅ 코드 병합 완료")
        return final_code
        
    finally:
        await connection.close()


async def _merge_blocks_sequentially(
    all_blocks: List[Dict[str, Any]],
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str | None
) -> str:
    """
    시작 라인 순서대로 블록을 순차 병합합니다.
    변환 로직(create_dbms_conversion.py)과 동일한 방식으로 동작합니다.
    """
    parent_stack = []  # 부모 스택
    merged_code = ""    # 최종 병합 코드
    
    for block in all_blocks:
        start_line = block.get('start_line', 0)
        end_line = block.get('end_line', 0)
        converted_code = block.get('converted_code', '').strip()
        has_children = block.get('has_children', False)
        
        # 부모 경계 체크: 범위를 벗어난 부모들을 마무리
        while parent_stack and start_line > parent_stack[-1]['end']:
            merged_code = _finalize_parent(parent_stack, merged_code)
        
        # 부모 노드인 경우: 스택에 추가
        if has_children:
            entry = {
                'start': start_line,
                'end': end_line,
                'code': converted_code,
                'children': []
            }
            parent_stack.append(entry)
            logger.debug(f"📦 부모 스택 push | 라인={start_line}~{end_line}")
        else:
            # 자식 노드인 경우: 현재 부모의 children에 추가
            if parent_stack:
                parent_stack[-1]['children'].append(converted_code)
                logger.debug(f"➕ 자식 추가 | 부모={parent_stack[-1]['start']}~{parent_stack[-1]['end']}")
            else:
                # 최상위 레벨: 바로 병합
                merged_code += f"\n{converted_code}"
                logger.debug(f"➕ 최상위 코드 추가 | 라인={start_line}~{end_line}")
    
    # 남은 부모들 마무리
    while parent_stack:
        merged_code = _finalize_parent(parent_stack, merged_code)
    
    return merged_code.strip()


def _finalize_parent(
    parent_stack: List[Dict[str, Any]],
    merged_code: str
) -> str:
    """현재 부모를 마무리하고 자식들을 치환합니다."""
    if not parent_stack:
        return merged_code
    
    entry = parent_stack.pop()
    code = entry['code']
    child_block = "\n".join(entry['children']).strip()
    
    # CODE_PLACEHOLDER가 있으면 치환, 없으면 끝에 추가
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
    
    # 상위 부모가 있으면 children에 추가, 없으면 최종 코드에 추가
    if parent_stack:
        parent_stack[-1]['children'].append(code)
        logger.debug(f"🔁 상위 부모 children에 merge | 라인={parent_stack[-1]['start']}~{parent_stack[-1]['end']}")
    else:
        merged_code += f"\n{code}"
        logger.debug("🧩 최상위 코드에 병합 완료")
    
    return merged_code

