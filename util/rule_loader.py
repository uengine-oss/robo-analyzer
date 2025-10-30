"""
Rule 로더 모듈
- Rule 파일(YAML) 기반 프롬프트 및 템플릿 관리
- 다국어/다타겟 언어 지원
- 성능 최적화 (캐싱)
- LLM 호출 및 템플릿 렌더링 통합
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from jinja2 import Template, TemplateError
from functools import lru_cache
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from util.llm_client import get_llm
from util.exception import LLMCallError


class RuleLoader:
    """
    Rule 파일 기반 통합 로더
    
    특징:
    - YAML 기반 프롬프트 및 템플릿 관리
    - Jinja2 템플릿 엔진 사용
    - 타겟 언어별 role 파일 지원 (java, python 등)
    - LRU 캐싱으로 성능 최적화
    - LLM 호출 및 템플릿 렌더링 통합
    - 입력값 검증
    """
    
    __slots__ = ('target_lang', 'role_dir', '_cache')
    
    def __init__(self, target_lang: str = 'java'):
        """
        RuleLoader 초기화
        
        Args:
            target_lang: 타겟 언어 (java, python 등)
        """
        self.target_lang = target_lang
        # role 파일 디렉터리 ('rules/<target_lang>')
        self.role_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'rules', target_lang)
        self._cache = {}
        
        if not os.path.exists(self.role_dir):
            raise FileNotFoundError(f"Role 디렉토리가 존재하지 않습니다: {self.role_dir}")
    
    @lru_cache(maxsize=32)
    def _load_role_file(self, role_name: str) -> Dict[str, Any]:
        """
        Role 파일 로드 (캐싱)
        
        Args:
            role_name: role 파일명
            
        Returns:
            Dict: Role 파일 내용
            
        Raises:
            FileNotFoundError: Role 파일이 존재하지 않을 때
        """
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
        """
        입력값 검증 및 기본값 설정
        
        Args:
            role: Role 파일 내용
            inputs: 입력 데이터
            
        Returns:
            Dict: 검증된 입력값
        """
        validated = inputs.copy()
        
        # 필수 필드 검증
        required_fields = role.get('input_schema', {}).get('required', [])
        for field in required_fields:
            if field not in validated:
                raise ValueError(f"필수 필드 누락: {field}")
        
        # 선택적 필드 기본값 설정
        optional_fields = role.get('input_schema', {}).get('optional', {})
        for field, spec in optional_fields.items():
            if field not in validated:
                if isinstance(spec, dict) and 'default' in spec:
                    validated[field] = spec['default']
        
        return validated
    
    def render_template(self, role_name: str, inputs: Dict[str, Any]) -> str:
        """
        템플릿 렌더링 (LLM 호출 없음)
        
        Args:
            role_name: role 파일명
            inputs: 템플릿 변수
        
        Returns:
            str: 렌더링된 템플릿
        """
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
        """
        프롬프트 템플릿 렌더링 (LLM 호출 없음)
        
        Args:
            role_name: role 파일명
            inputs: 템플릿 변수
        
        Returns:
            str: 렌더링된 프롬프트
        """
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
        """
        프롬프트 실행 (LLM 호출)
        
        Args:
            role_name: role 파일명
            inputs: 입력 데이터
            api_key: LLM API 키
        
        Returns:
            Dict: LLM 응답 (JSON 파싱됨)
        
        Raises:
            LLMCallError: LLM 호출 실패 시
        """
        try:
            _ = self._load_role_file(role_name)
            prompt_text = self.render_prompt(role_name, inputs)
            
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
            
            result = chain.invoke({"prompt": prompt_text})
            return result
            
        except Exception as e:
            err_msg = f"{role_name} 프롬프트 실행 중 오류: {str(e)}"
            logging.error(err_msg)
            raise LLMCallError(err_msg)
    
    def clear_cache(self):
        """캐시 초기화"""
        self._load_role_file.cache_clear()
        self._cache.clear()