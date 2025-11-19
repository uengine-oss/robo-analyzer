"""
오류 수정 메인 스크립트
- 컴파일 오류 발생 시 변환된 코드를 자동으로 수정하고 재병합
"""

import logging
import asyncio
from typing import Optional
from error_fix.error_parser import parse_error_message
from error_fix.block_finder import find_converting_node, find_block_by_line_number, get_block_with_children
from error_fix.code_fixer import fix_code_with_llm
from error_fix.code_merger import merge_fixed_code
from understand.neo4j_connection import Neo4jConnection
from util.utility_tool import escape_for_cypher
from convert.dbms.create_dbms_skeleton import start_dbms_skeleton
from util.exception import ConvertingError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def fix_conversion_error(
    error_message: str,
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str,
    api_key: str,
    locale: str = "ko",
    conversion_type: str = "dbms",
    target: str = "oracle"
) -> str:
    """
    컴파일 오류를 수정하고 변환된 코드를 재병합합니다.
    
    Args:
        error_message: 컴파일 오류 메시지 (예: "ORA-00942: table or view does not exist at line 10")
        folder_name: 폴더명
        file_name: 파일명
        procedure_name: 프로시저명
        user_id: 사용자 ID
        project_name: 프로젝트명
        api_key: LLM API 키
        locale: 언어 설정
        conversion_type: 변환 타입 ("dbms" 또는 "framework")
        target: 타겟 (예: "oracle", "java")
        
    Returns:
        수정 및 병합된 최종 코드
    """
    try:
        # 1. 오류 메시지 파싱
        logger.info("🔍 오류 메시지 파싱 중...")
        error_info = parse_error_message(error_message)
        if not error_info:
            raise ConvertingError("오류 메시지를 파싱할 수 없습니다.")
        
        error_number = error_info.get('error_number')
        error_code = error_info.get('error_code', 'UNKNOWN')
        error_msg = error_info.get('error_message', error_message)
        line_number = error_info.get('line_number')
        
        logger.info(f"✅ 오류 정보: {error_code} (라인: {line_number})")
        
        # 2. CONVERTING 노드 찾기
        logger.info("🔍 CONVERTING 노드 검색 중...")
        converting_node = await find_converting_node(
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            user_id=user_id,
            project_name=project_name,
            conversion_type=conversion_type,
            target=target
        )
        
        if not converting_node:
            raise ConvertingError(
                f"CONVERTING 노드를 찾을 수 없습니다: "
                f"{folder_name}/{file_name}/{procedure_name}"
            )
        
        logger.info("✅ CONVERTING 노드 찾음")
        
        # 3. 오류 라인 번호를 포함하는 블록 찾기 (자식 우선)
        if not line_number:
            raise ConvertingError("오류 메시지에서 라인 번호를 추출할 수 없습니다.")
        
        logger.info(f"🔍 오류 라인 {line_number}을 포함하는 블록 검색 중...")
        error_block = await find_block_by_line_number(
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            user_id=user_id,
            project_name=project_name,
            conversion_type=conversion_type,
            target=target,
            line_number=line_number
        )
        
        if not error_block:
            raise ConvertingError(
                f"라인 {line_number}을 포함하는 CONVERSION_BLOCK을 찾을 수 없습니다."
            )
        
        logger.info(
            f"✅ 오류 블록 찾음: 라인 {error_block.get('start_line')}~{error_block.get('end_line')}"
        )
        
        # 4. 블록과 자식 블록 정보 가져오기
        block_start = error_block.get('start_line')
        block_end = error_block.get('end_line')
        block_info = await get_block_with_children(
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            user_id=user_id,
            project_name=project_name,
            conversion_type=conversion_type,
            target=target,
            block_start_line=block_start,
            block_end_line=block_end
        )
        
        target_block = block_info.get('block')
        children = block_info.get('children', [])
        
        # 5. LLM으로 코드 수정
        original_code = target_block.get('original_code', '')
        converted_code = target_block.get('converted_code', '')
        
        logger.info("🤖 LLM을 통한 코드 수정 중...")
        fixed_code = await fix_code_with_llm(
            original_code=original_code,
            converted_code=converted_code,
            error_message=error_msg,
            error_code=error_code,
            error_number=error_number,
            api_key=api_key,
            locale=locale,
            conversion_type=conversion_type,
            target=target
        )
        
        # 6. Neo4j에 수정된 코드 업데이트
        logger.info("💾 Neo4j에 수정된 코드 저장 중...")
        await update_block_code(
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            user_id=user_id,
            project_name=project_name,
            start_line=block_start,
            end_line=block_end,
            fixed_code=fixed_code
        )
        
        logger.info("✅ 블록 코드 업데이트 완료")
        
        # 7. 스켈레톤 코드 가져오기 (재생성)
        logger.info("🔧 스켈레톤 코드 생성 중...")
        skeleton_code = await start_dbms_skeleton(
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            project_name=project_name,
            user_id=user_id,
            api_key=api_key,
            locale=locale,
            target_dbms=target
        )
        
        # 8. 코드 병합
        logger.info("🔗 코드 병합 중...")
        merged_code = await merge_fixed_code(
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            user_id=user_id,
            project_name=project_name,
            conversion_type=conversion_type,
            target=target,
            skeleton_code=skeleton_code
        )
        
        logger.info("✅ 오류 수정 및 코드 병합 완료!")
        return merged_code
        
    except Exception as e:
        logger.error(f"❌ 오류 수정 실패: {str(e)}")
        raise ConvertingError(f"오류 수정 중 오류: {str(e)}")


async def update_block_code(
    folder_name: str,
    file_name: str,
    procedure_name: str,
    user_id: str,
    project_name: str | None,
    start_line: int,
    end_line: int,
    fixed_code: str
) -> None:
    """
    Neo4j의 CONVERSION_BLOCK 노드에 수정된 코드를 업데이트합니다.
    """
    connection = Neo4jConnection()
    try:
        project_condition = f", project_name: '{escape_for_cypher(project_name)}'" if project_name else ""
        escaped_code = escape_for_cypher(fixed_code)
        
        query = f"""
            MATCH (block:CONVERSION_BLOCK {{
                folder_name: '{escape_for_cypher(folder_name)}',
                file_name: '{escape_for_cypher(file_name)}',
                procedure_name: '{escape_for_cypher(procedure_name)}',
                user_id: '{escape_for_cypher(user_id)}'{project_condition},
                start_line: {start_line},
                end_line: {end_line}
            }})
            SET block.converted_code = '{escaped_code}',
                block.updated_at = datetime()
        """
        
        await connection.execute_queries([query])
    finally:
        await connection.close()


# CLI 진입점
async def main():
    """
    CLI에서 실행할 때 사용하는 메인 함수
    예: python -m error_fix.main
    """
    import sys
    
    if len(sys.argv) < 8:
        print("사용법: python -m error_fix.main <error_message> <folder_name> <file_name> <procedure_name> <user_id> <project_name> <api_key> [locale] [conversion_type] [target]")
        sys.exit(1)
    
    error_message = sys.argv[1]
    folder_name = sys.argv[2]
    file_name = sys.argv[3]
    procedure_name = sys.argv[4]
    user_id = sys.argv[5]
    project_name = sys.argv[6]
    api_key = sys.argv[7]
    locale = sys.argv[8] if len(sys.argv) > 8 else "ko"
    conversion_type = sys.argv[9] if len(sys.argv) > 9 else "dbms"
    target = sys.argv[10] if len(sys.argv) > 10 else "oracle"
    
    try:
        fixed_code = await fix_conversion_error(
            error_message=error_message,
            folder_name=folder_name,
            file_name=file_name,
            procedure_name=procedure_name,
            user_id=user_id,
            project_name=project_name,
            api_key=api_key,
            locale=locale,
            conversion_type=conversion_type,
            target=target
        )
        
        print("\n" + "="*80)
        print("✅ 수정된 코드:")
        print("="*80)
        print(fixed_code)
        print("="*80)
        
    except Exception as e:
        print(f"❌ 오류: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

