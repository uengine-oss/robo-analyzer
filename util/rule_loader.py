import json
import os
import yaml
import logging
from typing import Dict, Any
from jinja2 import Template, TemplateError
from functools import lru_cache
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import JsonOutputParser
from util.llm_client import get_llm
from util.llm_audit import invoke_with_audit
from util.exception import LLMCallError


# ===== Helpers =====
def _safe_copy(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False))
    except (TypeError, ValueError):
        return value


# ===== RuleLoader =====
class RuleLoader:
    """YAML 기반 프롬프트/템플릿 로더."""
    
    __slots__ = ('target_lang', 'domain', 'role_dir')
    
    def __init__(self, target_lang: str = 'java', domain: str = 'understand'):
        """RuleLoader 초기화."""
        self.target_lang = target_lang
        self.domain = domain
        self.role_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'rules',
            domain,
            target_lang
        )
        if not os.path.exists(self.role_dir):
            raise FileNotFoundError(f"Role 디렉토리가 존재하지 않습니다: {self.role_dir}")
    
    @lru_cache(maxsize=32)
    def _load_role_file(self, role_name: str) -> Dict[str, Any]:
        """Role 파일 로드 (캐싱)."""
        role_path = os.path.join(self.role_dir, f"{role_name}.yaml")
        if not os.path.exists(role_path):
            raise FileNotFoundError(f"Role 파일이 존재하지 않습니다: {role_path}")
        
        try:
            with open(role_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"YAML 파싱 오류 ({role_path}): {str(e)}")
        except Exception as e:
            raise ValueError(f"Role 파일 로드 오류 ({role_path}): {str(e)}")
    
    def validate_inputs(self, role: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
        """입력값 검증 및 기본값 설정."""
        validated = inputs.copy()
        required_fields = role.get('input_schema', {}).get('required', [])
        for field in required_fields:
            if field not in validated:
                raise ValueError(f"필수 필드 누락: {field}")
        
        optional_fields = role.get('input_schema', {}).get('optional', {})
        for field, spec in optional_fields.items():
            if field not in validated and isinstance(spec, dict) and 'default' in spec:
                validated[field] = spec['default']
        
        return validated
    
    def render_template(self, role_name: str, inputs: Dict[str, Any]) -> str:
        """템플릿 렌더링 (LLM 미호출)."""
        role = self._load_role_file(role_name)
        validated_inputs = self.validate_inputs(role, inputs)
        
        try:
            # Jinja2 템플릿 렌더링
            template_content = role.get('template', '')
            if not template_content:
                raise ValueError(f"템플릿이 정의되지 않았습니다: {role_name}")
            
            template = Template(template_content)
            return template.render(**validated_inputs)
        except TemplateError as e:
            raise ValueError(f"템플릿 렌더링 오류 ({role_name}): {str(e)}")
        except KeyError as e:
            raise ValueError(f"템플릿에 필요한 키 누락 ({role_name}): {str(e)}")
    
    def render_prompt(self, role_name: str, inputs: Dict[str, Any]) -> str:
        """프롬프트 템플릿 렌더링 (LLM 미호출)."""
        role = self._load_role_file(role_name)
        validated_inputs = self.validate_inputs(role, inputs)
        
        try:
            # Jinja2 템플릿 렌더링
            template = Template(role['prompt'])
            return template.render(**validated_inputs)
        except TemplateError as e:
            raise ValueError(f"프롬프트 템플릿 렌더링 오류 ({role_name}): {str(e)}")
        except KeyError as e:
            raise ValueError(f"프롬프트 템플릿에 필요한 키 누락 ({role_name}): {str(e)}")
    
    def execute(self, role_name: str, inputs: Dict[str, Any], api_key: str) -> Dict[str, Any]:
        """프롬프트 실행 (LLM 호출)."""
        try:
            _ = self._load_role_file(role_name)
            prompt_text = self.render_prompt(role_name, inputs)

            role_path = os.path.join(self.role_dir, f"{role_name}.yaml")

            # LLM 호출 (파라미터는 llm_client에서 일괄 관리)
            llm = get_llm(api_key=api_key)

            # Langchain 체인 구성
            langchain_prompt = PromptTemplate.from_template("{prompt}")
            chain = (
                RunnablePassthrough()
                | langchain_prompt
                | llm
                | JsonOutputParser()
            )

            payload = {"prompt": prompt_text}
            metadata = {
                "role": role_name,
                "targetLang": self.target_lang,
                "domain": self.domain,
                "rulePath": role_path,
            }

            result = invoke_with_audit(
                chain,
                payload,
                prompt_name=f"rules/{self.domain}/{self.target_lang}/{role_name}",
                input_payload={
                    "inputs": _safe_copy(inputs),
                    "renderedPrompt": prompt_text,
                },
                metadata=metadata,
            )
            return result
            
        except Exception as e:
            err_msg = f"{role_name} 프롬프트 실행 중 오류: {e}"
            logging.error(err_msg, exc_info=True)
            raise LLMCallError(err_msg)
    
    def clear_cache(self):
        """캐시 초기화"""
        self._load_role_file.cache_clear()