import logging
import os
import textwrap
import tiktoken
from prompt.convert_controller_prompt import convert_controller_method_code
from util.exception import ControllerCreationError, ConvertingError, FilePathError, LLMCallError, ProcessResultError, SaveFileError
from util.file_utils import save_file

encoder = tiktoken.get_encoding("cl100k_base")
CONTROLLER_PATH = 'java/demo/src/main/java/com/example/demo/controller'


# 역할: 생성된 컨트롤러 코드를 지정된 경로에 Java 파일로 저장하는 함수입니다.
#      Docker 환경 여부에 따라 적절한 저장 경로를 선택하고,
#
# 매개변수:
#   - controller_skeleton : 전체 컨트롤러 클래스의 기본 구조 템플릿
#   - controller_class_name : 생성할 컨트롤러 클래스의 이름
#   - merge_controller_method_code : 컨트롤러 클래스에 추가될 메서드 코드
async def generate_controller_class(controller_skeleton: str, controller_class_name: str, merge_controller_method_code: str):
    try:
        # * 컨트롤러 코드 생성
        merge_controller_method_code = textwrap.indent(merge_controller_method_code.strip(), '        ')
        controller_code = controller_skeleton.replace("CodePlaceHolder", merge_controller_method_code)

        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), CONTROLLER_PATH)
        else:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(project_root, 'target', CONTROLLER_PATH)

        # * 파일 저장
        await save_file(content=controller_code, filename=f"{controller_class_name}.java", base_path=save_path)
        logging.info(f"[{controller_class_name}] Success Create Controller Java File\n")

    except SaveFileError:
        raise
    except Exception:   
        err_msg = "컨트롤러 클래스 파일 경로를 설정하는 도중 오류가 발생했습니다."
        logging.error(err_msg)
        raise FilePathError(err_msg)
    
    
# 역할: 컨트롤러 메서드 코드를 생성하는 함수입니다.
#      LLM을 통해 생성된 컨트롤러 메서드 코드를 처리합니다.
#
# 매개변수: 
#   - method_signature: 서비스 메서드의 시그니처
#   - procedure_name: 원본 프로시저/함수 이름
#   - command_class_variable: Command DTO 필드 목록
#   - command_class_name: Command 클래스 이름
#   - controller_skeleton: 컨트롤러 클래스 기본 구조
#
# 반환값: 
#   - method_skeleton_code: 생성된 컨트롤러 메서드 코드
async def process_controller_method_code(method_signature: str, procedure_name: str, command_class_variable: str, command_class_name: str, controller_skeleton: str) -> str:

    try:
        # * 컨트롤러 메서드 틀 생성에 필요한 정보를 받습니다.
        analysis_method = convert_controller_method_code(
            method_signature,
            procedure_name,
            command_class_variable,
            command_class_name,
            controller_skeleton
        )
        method_skeleton_code = analysis_method['method']
        return method_skeleton_code

    except (LLMCallError):
        raise
    except Exception:
        err_msg = "컨트롤러 메서드를 생성하는 과정에서 결과 처리 준비 처리를 하는 도중 문제가 발생했습니다."
        logging.error(err_msg)
        raise ProcessResultError(err_msg)


# 역할: 컨트롤러 메서드 생성 프로세스를 시작하고 관리하는 함수입니다.
#      컨트롤러 메서드 생성에 필요한 모든 단계를 조율하고 실행합니다.
# 매개변수:
#   - method_signature: 서비스 메서드의 시그니처
#   - procedure_name: 원본 프로시저/함수 이름
#   - command_class_variable: Command DTO 필드 목록
#   - command_class_name: Command 클래스 이름
#   - merge_controller_method_code: 병합될 컨트롤러 메서드 코드
#   - controller_skeleton: 컨트롤러 클래스 기본 구조
#   - object_name: 대상 객체 이름 (로깅용)
#   - node_type: 대상 노드 타입
# 반환값:
#   - controller_method_code: 생성된 컨트롤러 메서드 코드
async def start_controller_processing(method_signature: str, procedure_name: str, command_class_variable: str, command_class_name: str, node_type: str, merge_controller_method_code: str, controller_skeleton: str, object_name: str) -> str:

    logging.info(f"[{object_name}] {procedure_name} 프로시저의 컨트롤러 생성을 시작합니다.")

    try:
        # * FUNCTION 타입이 아닐 때만 컨트롤러 메서드 생성
        if node_type != "FUNCTION":

            # * 컨트롤러 메서드 생성을 시작합니다.
            controller_method_code = await process_controller_method_code(
                method_signature, 
                procedure_name, 
                command_class_variable,
                command_class_name,
                controller_skeleton
            )

            logging.info(f"[{object_name}] {procedure_name} 프로시저의 컨트롤러 메서드 생성 완료\n")
            merge_controller_method_code = f"{merge_controller_method_code}\n\n{controller_method_code}"

        return merge_controller_method_code

    except ConvertingError:
        raise
    except Exception:
        err_msg = "컨트롤러 메서드를 생성하기 위해 데이터를 준비하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ControllerCreationError(err_msg)
