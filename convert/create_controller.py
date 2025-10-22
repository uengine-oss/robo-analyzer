import logging
import textwrap
import json
from util.exception import ConvertingError
from util.utility_tool import save_file, build_java_base_path, convert_to_camel_case, convert_to_pascal_case
from util.prompt_loader import PromptLoader


# ----- 상수 정의 -----
CODE_PLACEHOLDER = "CodePlaceHolder"
SKIP_NODE_TYPE = "FUNCTION"


# ----- 컨트롤러 생성 클래스 -----
class ControllerGenerator:
    """
    컨트롤러 인터페이스 생성
    - 여러 프로시저의 메서드를 하나의 Controller로 통합
    - Generator 방식으로 통일
    """
    __slots__ = (
        'project_name', 'user_id', 'api_key', 'locale', 'prompt_loader', 'save_path'
    )

    def __init__(self, project_name: str, user_id: str, api_key: str, locale: str = 'ko', target_lang: str = 'java'):
        """
        ControllerGenerator 초기화
        
        Args:
            project_name: 프로젝트 이름
            user_id: 사용자 식별자
            api_key: LLM API 키
            locale: 언어 설정
            target_lang: 타겟 언어
        """
        self.project_name = project_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.prompt_loader = PromptLoader(target_lang=target_lang)
        self.save_path = build_java_base_path(project_name, user_id, 'controller')
    
    async def generate(self, object_name: str, service_class_name: str, exist_command_class: bool,
                      service_creation_info: list) -> tuple[str, str]:
        """
        Controller 클래스 생성 (Skeleton + 메서드)
        
        Args:
            object_name: 객체 이름
            service_class_name: Service 클래스 이름 (import용)
            exist_command_class: Command 클래스 존재 여부
            service_creation_info: Service 메서드 정보 리스트
        
        Returns:
            tuple: (controller_class_name, controller_code)
        """
        logging.info("\n" + "="*80)
        logging.info(f"🌐 STEP 4: Controller 생성 - {object_name}")
        logging.info("="*80)
        
        # Controller Skeleton 생성
        pascal_name = convert_to_pascal_case(object_name)
        camel_name = convert_to_camel_case(object_name)
        controller_class_name = f"{pascal_name}Controller"
        
        # Service 클래스명 (전달받거나 기본값)
        service_class_name = service_class_name or f"{pascal_name}Service"
        service_var_name = service_class_name[0].lower() + service_class_name[1:]
        
        # Command import (조건부)
        command_import = (
            f"import com.example.{self.project_name}.command.{camel_name}.*;\n"
            if exist_command_class else ""
        )

        # 컨트롤러 템플릿
        controller_skeleton = f"""package com.example.{self.project_name}.controller;

{command_import}import com.example.{self.project_name}.service.{service_class_name};
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

@RestController
@RequestMapping("/{camel_name}")
public class {controller_class_name} {{

    @Autowired
    private {service_class_name} {service_var_name};

{CODE_PLACEHOLDER}
}}"""
        
        # 각 프로시저별 메서드 생성
        controller_methods = []
        
        for svc in service_creation_info:
            method_sig = svc['method_signature']
            proc_name = svc['procedure_name']
            cmd_var = svc['command_class_variable']
            cmd_name = svc['command_class_name']
            node_type = svc['node_type']
            
            # FUNCTION 타입 스킵
            if node_type == SKIP_NODE_TYPE:
                logging.info(f"  ⏭️  {proc_name} FUNCTION 타입 스킵")
                continue
            
            logging.info(f"  📌 Controller 메서드: {proc_name}")
            
            # LLM으로 메서드 생성 (Role 파일 사용)
            result = self.prompt_loader.execute(
                role_name='controller',
                inputs={
                    'method_signature': method_sig,
                    'procedure_name': proc_name,
                    'command_class_variable': json.dumps(cmd_var, ensure_ascii=False, indent=2),
                    'command_class_name': cmd_name,
                    'controller_skeleton': controller_skeleton,
                    'locale': self.locale
                },
                api_key=self.api_key
            )
            
            controller_methods.append(result['method'])
            logging.info(f"  ✅ {proc_name} 메서드 생성 완료")
        
        # Controller 파일 조립 및 저장
        merged_methods = '\n\n'.join(controller_methods)
        completed = controller_skeleton.replace(
            CODE_PLACEHOLDER,
            textwrap.indent(merged_methods.strip(), '    ')
        )
        
        await save_file(
            content=completed,
            filename=f"{controller_class_name}.java",
            base_path=self.save_path
        )
        
        logging.info(f"\n💾 Controller 파일 저장 완료: {controller_class_name}.java")
        logging.info(f"   경로: {self.save_path}")
        
        logging.info("\n" + "-"*80)
        logging.info(f"✅ STEP 4 완료: Controller 생성 완료")
        logging.info("-"*80 + "\n")
        
        return controller_class_name, completed


# ----- 진입점 함수 -----
def start_controller_skeleton_processing(
    object_name: str,
    exist_command_class: bool,
    project_name: str,
    service_class_name: str = None
) -> tuple[str, str]:
    """
    컨트롤러 스켈레톤 생성 시작 (호환성을 위한 함수)
    
    Args:
        object_name: 패키지/객체 이름
        exist_command_class: Command 클래스 존재 여부
        project_name: 프로젝트 이름
        service_class_name: Service 클래스 이름 (import용)
    
    Returns:
        tuple: (controller_skeleton, controller_class_name)
    
    Raises:
        ConvertingError: 생성 중 오류 발생 시
    """
    try:
        pascal_name = convert_to_pascal_case(object_name)
        camel_name = convert_to_camel_case(object_name)
        controller_class_name = f"{pascal_name}Controller"
        
        # Service 클래스명 (전달받거나 기본값)
        service_class_name = service_class_name or f"{pascal_name}Service"
        service_var_name = service_class_name[0].lower() + service_class_name[1:]
        
        # Command import (조건부)
        command_import = (
            f"import com.example.{project_name}.command.{camel_name}.*;\n"
            if exist_command_class else ""
        )

        # 컨트롤러 템플릿
        controller_skeleton = f"""package com.example.{project_name}.controller;

{command_import}import com.example.{project_name}.service.{service_class_name};
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

@RestController
@RequestMapping("/{camel_name}")
public class {controller_class_name} {{

    @Autowired
    private {service_class_name} {service_var_name};

{CODE_PLACEHOLDER}
}}"""
        
        logging.info(f"[{object_name}] 컨트롤러 스켈레톤 생성 완료\n")
        return controller_skeleton, controller_class_name

    except Exception as e:
        err_msg = f"컨트롤러 스켈레톤 생성 중 오류: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
