"""
LLM을 통한 코드 수정
- 원본 코드, 변환된 코드, 컴파일 오류를 받아 수정된 코드 생성
"""

import logging
from typing import Dict, Any
from util.llm_client import get_llm
from util.llm_audit import invoke_with_audit
from util.exception import LLMCallError

logger = logging.getLogger(__name__)


async def fix_code_with_llm(
    original_code: str,
    converted_code: str,
    error_message: str,
    error_code: str,
    error_number: int | None,
    api_key: str,
    locale: str = "ko",
    conversion_type: str = "dbms",
    target: str = "oracle"
) -> str:
    """
    LLM을 사용하여 오류가 있는 변환된 코드를 수정합니다.
    
    Args:
        original_code: 원본 코드
        converted_code: 변환된 코드 (오류 발생)
        error_message: 컴파일 오류 메시지
        error_code: 오류 코드 (예: "ORA-00942")
        error_number: 오류 번호 (예: 942)
        api_key: LLM API 키
        locale: 언어 설정
        conversion_type: 변환 타입 ("dbms" 또는 "framework")
        target: 타겟 (예: "oracle", "java")
        
    Returns:
        수정된 코드
    """
    # 프롬프트 구성
    prompt = f"""당신은 {target.upper()} 코드 수정 전문가입니다.
컴파일 오류가 발생한 변환된 코드를 수정해야 합니다.

[원본 코드]
{original_code}

[변환된 코드 (오류 발생)]
{converted_code}

[컴파일 오류]
오류 코드: {error_code}
오류 번호: {error_number if error_number else "N/A"}
오류 메시지: {error_message}

[요구사항]
1. 변환된 코드에서 오류를 수정하세요.
2. 원본 코드의 의도를 유지하면서 {target.upper()} 문법에 맞게 수정하세요.
3. 오류 메시지를 참고하여 정확한 수정을 수행하세요.
4. 수정된 코드만 반환하세요 (설명 없이 코드만).

[수정된 코드]
"""

    try:
        llm = get_llm(api_key=api_key)
        
        result = await invoke_with_audit(
            llm=llm,
            messages=[{"role": "user", "content": prompt}],
            model_name="gpt-4.1",  # 필요시 파라미터로 받을 수 있음
            api_key=api_key
        )
        
        fixed_code = result.content.strip()
        
        # 코드 블록 마커 제거 (```python, ``` 등)
        if fixed_code.startswith("```"):
            lines = fixed_code.split("\n")
            # 첫 줄과 마지막 줄이 코드 블록 마커인 경우 제거
            if lines[0].startswith("```") and lines[-1].strip() == "```":
                fixed_code = "\n".join(lines[1:-1]).strip()
            elif lines[0].startswith("```"):
                fixed_code = "\n".join(lines[1:]).strip()
        
        logger.info(f"✅ 코드 수정 완료 (오류: {error_code})")
        return fixed_code
        
    except Exception as e:
        logger.error(f"❌ 코드 수정 실패: {str(e)}")
        raise LLMCallError(f"코드 수정 중 오류: {str(e)}")

