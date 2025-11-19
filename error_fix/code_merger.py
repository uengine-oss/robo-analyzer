"""
코드 병합 로직
- 수정된 블록을 기존 코드 구조에 병합
- 부모-자식 관계를 고려하여 병합
"""

import logging
from typing import Dict, Any, List, Optional
from understand.neo4j_connection import Neo4jConnection
from util.utility_tool import escape_for_cypher

logger = logging.getLogger(__name__)


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
        # CONVERTING 노드의 모든 블록을 가져오기 (NEXT 관계 순서대로)
        project_condition = f", project_name: '{escape_for_cypher(project_name)}'" if project_name else ""
        
        # 최상위 블록들 찾기 (부모가 없는 블록들)
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
            WHERE NOT EXISTS((:CONVERSION_BLOCK)-[:PARENT_OF]->(block))
            WITH block
            ORDER BY block.start_line
            OPTIONAL MATCH (block)-[:NEXT]->(next:CONVERSION_BLOCK {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}'{project_condition}
            }})
            WITH block, next
            ORDER BY block.start_line
            RETURN block, next
        """
        
        results = await connection.execute_queries([query])
        
        if not results or len(results) == 0 or len(results[0]) == 0:
            logger.warning("병합할 블록을 찾지 못했습니다.")
            return skeleton_code.replace("CodePlaceHolder", "")
        
        # 최상위 블록들을 순서대로 가져오기
        top_level_blocks = []
        for row in results[0]:
            block = dict(row.get('block'))
            if block:
                top_level_blocks.append(block)
        
        # 각 블록의 자식들을 가져와서 병합
        merged_code = await _merge_blocks_with_children(
            top_level_blocks, 
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


async def _merge_blocks_with_children(
    top_level_blocks: List[Dict[str, Any]],
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str | None
) -> str:
    """
    최상위 블록들을 순서대로 병합합니다.
    각 블록의 자식들을 재귀적으로 가져와서 부모에 치환합니다.
    """
    connection = Neo4jConnection()
    try:
        merged_parts = []
        project_condition = f", project_name: '{escape_for_cypher(project_name)}'" if project_name else ""
        
        for block in top_level_blocks:
            block_start = block.get('start_line')
            block_end = block.get('end_line')
            converted_code = block.get('converted_code', '').strip()
            
            # 자식 블록들 찾기
            children_query = f"""
                MATCH (parent:CONVERSION_BLOCK {{
                    folder_name: '{escape_for_cypher(folder_name)}',
                    file_name: '{escape_for_cypher(file_name)}',
                    procedure_name: '{escape_for_cypher(procedure_name)}',
                    user_id: '{escape_for_cypher(user_id)}'{project_condition},
                    start_line: {block_start},
                    end_line: {block_end}
                }})-[:PARENT_OF]->(child:CONVERSION_BLOCK {{
                    folder_name: '{escape_for_cypher(folder_name)}',
                    file_name: '{escape_for_cypher(file_name)}',
                    procedure_name: '{escape_for_cypher(procedure_name)}',
                    user_id: '{escape_for_cypher(user_id)}'{project_condition}
                }})
                OPTIONAL MATCH (child)-[:NEXT]->(next:CONVERSION_BLOCK {{
                    folder_name: '{escape_for_cypher(folder_name)}',
                    file_name: '{escape_for_cypher(file_name)}',
                    procedure_name: '{escape_for_cypher(procedure_name)}',
                    user_id: '{escape_for_cypher(user_id)}'{project_condition}
                }})
                WITH child
                ORDER BY child.start_line
                RETURN child
            """
            
            children_results = await connection.execute_queries([children_query])
            children = []
            if children_results and len(children_results) > 0 and len(children_results[0]) > 0:
                children = [dict(row.get('child')) for row in children_results[0] if row.get('child')]
            
            # 자식이 있는 경우: 자식들을 재귀적으로 병합한 후 부모에 치환
            if children:
                # 자식들을 NEXT 관계 순서대로 정렬
                sorted_children = sorted(children, key=lambda c: c.get('start_line', 0))
                children_code_parts = []
                
                for child in sorted_children:
                    # 자식의 자식도 재귀적으로 처리 (간단화: 1단계만)
                    child_code = child.get('converted_code', '').strip()
                    children_code_parts.append(child_code)
                
                children_code = "\n".join(children_code_parts)
                
                # 부모 코드에 자식 코드를 치환 (CodePlaceHolder 또는 적절한 위치)
                if "CodePlaceHolder" in converted_code:
                    parent_merged = converted_code.replace("CodePlaceHolder", children_code)
                else:
                    # 부모 코드 끝에 자식 코드 추가
                    parent_merged = f"{converted_code}\n{children_code}"
                
                merged_parts.append(parent_merged)
            else:
                # 자식이 없는 경우: 그냥 추가
                merged_parts.append(converted_code)
        
        return "\n".join(merged_parts)
    finally:
        await connection.close()

