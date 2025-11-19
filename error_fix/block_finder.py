"""
Neo4j에서 CONVERSION_BLOCK 찾기
- 오류 번호 범위를 포함하는 블록을 찾음 (자식 노드 우선)
"""

import logging
from typing import Optional, List, Dict, Any
from understand.neo4j_connection import Neo4jConnection
from util.utility_tool import escape_for_cypher

logger = logging.getLogger(__name__)


async def find_converting_node(
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str,
    conversion_type: str,
    target: str
) -> Optional[Dict[str, Any]]:
    """
    CONVERTING 루트 노드를 찾습니다.
    
    Returns:
        CONVERTING 노드 정보 또는 None
    """
    connection = Neo4jConnection()
    try:
        query = f"""
            MATCH (conv:CONVERTING {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}',
                conversion_type: '{escape_for_cypher(conversion_type)}',
                target: '{escape_for_cypher(target)}'
            }})
            RETURN conv
            LIMIT 1
        """
        
        if project_name:
            query = f"""
                MATCH (conv:CONVERTING {{
                    folder_name: '{escape_for_cypher(folder_name)}',
                    file_name: '{escape_for_cypher(file_name)}',
                    procedure_name: '{escape_for_cypher(procedure_name)}',
                    user_id: '{escape_for_cypher(user_id)}',
                    project_name: '{escape_for_cypher(project_name)}',
                    conversion_type: '{escape_for_cypher(conversion_type)}',
                    target: '{escape_for_cypher(target)}'
                }})
                RETURN conv
                LIMIT 1
            """
        
        results = await connection.execute_queries([query])
        if results and len(results) > 0 and len(results[0]) > 0:
            conv_data = results[0][0].get('conv')
            if conv_data:
                return dict(conv_data)
        return None
    finally:
        await connection.close()


async def find_block_by_line_number(
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str,
    conversion_type: str,
    target: str,
    line_number: int
) -> Optional[Dict[str, Any]]:
    """
    특정 라인 번호를 포함하는 CONVERSION_BLOCK을 찾습니다.
    자식 노드를 우선으로 찾습니다 (더 구체적인 범위).
    
    Args:
        line_number: 오류가 발생한 라인 번호
        
    Returns:
        가장 구체적인 (자식) 블록 정보 또는 None
    """
    connection = Neo4jConnection()
    try:
        # project_name 조건
        project_condition = f", project_name: '{escape_for_cypher(project_name)}'" if project_name else ""
        
        # 1단계: 해당 라인을 포함하는 모든 블록 찾기 (자식 우선 정렬)
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
            WHERE block.start_line <= {line_number} AND block.end_line >= {line_number}
            OPTIONAL MATCH (parent:CONVERSION_BLOCK)-[:PARENT_OF]->(block)
            WITH block, parent,
                CASE WHEN parent IS NOT NULL THEN 1 ELSE 0 END as has_parent,
                (block.end_line - block.start_line) as block_size
            ORDER BY has_parent DESC, block_size ASC
            RETURN block, has_parent
            LIMIT 1
        """
        
        results = await connection.execute_queries([query])
        if results and len(results) > 0 and len(results[0]) > 0:
            block_data = results[0][0].get('block')
            if block_data:
                return dict(block_data)
        return None
    finally:
        await connection.close()


async def get_block_with_children(
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str,
    conversion_type: str,
    target: str,
    block_start_line: int,
    block_end_line: int
) -> Dict[str, Any]:
    """
    특정 블록과 그 자식 블록들을 모두 가져옵니다.
    
    Returns:
        {
            'block': 블록 정보,
            'children': 자식 블록 리스트 (NEXT 관계 순서대로)
        }
    """
    connection = Neo4jConnection()
    try:
        project_condition = f", project_name: '{escape_for_cypher(project_name)}'" if project_name else ""
        
        query = f"""
            MATCH (block:CONVERSION_BLOCK {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}'{project_condition},
                start_line: {block_start_line},
                end_line: {block_end_line}
            }})
            OPTIONAL MATCH path = (block)-[:PARENT_OF*]->(child:CONVERSION_BLOCK {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}'{project_condition}
            }})
            WITH block, collect(DISTINCT child) as children
            RETURN block, children
        """
        
        results = await connection.execute_queries([query])
        if results and len(results) > 0 and len(results[0]) > 0:
            row = results[0][0]
            block_data = dict(row.get('block'))
            children_data = [dict(c) for c in row.get('children', [])] if row.get('children') else []
            
            # 자식들을 NEXT 관계 순서대로 정렬
            sorted_children = _sort_children_by_next(children_data, folder_name, file_name, procedure_name, user_id, project_name)
            
            return {
                'block': block_data,
                'children': sorted_children
            }
        
        return {'block': None, 'children': []}
    finally:
        await connection.close()


def _sort_children_by_next(
    children: List[Dict[str, Any]],
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str | None
) -> List[Dict[str, Any]]:
    """
    자식 블록들을 NEXT 관계 순서대로 정렬합니다.
    (간단한 구현: start_line 기준 정렬)
    """
    return sorted(children, key=lambda c: c.get('start_line', 0))

