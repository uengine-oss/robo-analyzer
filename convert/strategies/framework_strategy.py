from typing import AsyncGenerator, Any
from .base_strategy import ConversionStrategy
from util.utility_tool import emit_message, emit_data, emit_error, emit_status

# 프레임워크 변환에 필요한 생성기/유틸 의존성들
from convert.framework.create_entity import EntityGenerator
from convert.framework.create_repository import RepositoryGenerator
from convert.framework.create_service_skeleton import ServiceSkeletonGenerator
from convert.framework.create_service_preprocessing import start_service_preprocessing
from convert.framework.create_controller import ControllerGenerator
from convert.framework.create_config_files import ConfigFilesGenerator
from convert.framework.create_main import MainClassGenerator


class FrameworkConversionStrategy(ConversionStrategy):
    """프레임워크 변환 전략 (Java Spring Boot, Python FastAPI 등)"""
    
    def __init__(self, target: str = "java"):
        self.target = target.lower()
    
    # 공통 컨텍스트 보관용 필드
    project_name: str | None = None
    user_id: str | None = None
    api_key: str | None = None
    locale: str | None = None
    
    async def convert(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """
        프레임워크 변환을 수행합니다.
        - Java: Spring Boot (엔티티 → 리포지토리 → 서비스 → 컨트롤러 → 설정/메인)
        - Python: FastAPI (TODO)
        """
        if self.target not in ("java", "python"):
            yield emit_error(f"지원하지 않는 대상입니다: {self.target}")
            return
        
        if self.target == "python":
            yield emit_error("Python FastAPI 변환은 아직 지원되지 않습니다")
            return

        # 공통 컨텍스트 저장
        self.project_name = orchestrator.project_name
        self.user_id = orchestrator.user_id
        self.api_key = orchestrator.api_key
        self.locale = orchestrator.locale

        total_files = len(file_names)
        target_name = "Spring Boot" if self.target == "java" else "FastAPI"

        try:
            yield emit_data(file_type="project_name", project_name=self.project_name)

            yield emit_message(f"{target_name} 프레임워크 변환을 시작합니다")
            yield emit_message(f"프로젝트 '{self.project_name}'의 {total_files}개 파일을 변환합니다")

            yield emit_message("엔티티 클래스 생성을 시작합니다")
            yield emit_message("테이블 정보를 기반으로 JPA 엔티티를 생성하고 있습니다")
            
            entity_result_list = await self._step_entity()
            entity_count = len(entity_result_list)
            
            for idx, entity in enumerate(entity_result_list, 1):
                entity_name, entity_code = entity["entityName"], entity["entityCode"]
                yield emit_message(f"엔티티 생성 완료: {entity_name}.java ({idx}/{entity_count})")
                yield emit_data(
                    file_type="entity_class",
                    file_name=f"{entity_name}.java",
                    code=entity_code,
                )
            yield emit_message(f"엔티티 클래스 생성이 완료되었습니다 (총 {entity_count}개)")
            yield emit_status(1, done=True)

            yield emit_message("리포지토리 인터페이스 생성을 시작합니다")
            yield emit_message("데이터 접근 레이어 코드를 생성하고 있습니다")
            
            (
                used_query_methods,
                global_variables,
                sequence_methods,
                repository_list,
            ) = await self._step_repository()
            repo_count = len(repository_list)
            
            for idx, repo in enumerate(repository_list, 1):
                repo_name, repo_code = repo["repositoryName"], repo["code"]
                yield emit_message(f"리포지토리 생성 완료: {repo_name}.java ({idx}/{repo_count})")
                yield emit_data(
                    file_type="repository_class",
                    file_name=f"{repo_name}.java",
                    code=repo_code,
                )
            yield emit_message(f"리포지토리 생성이 완료되었습니다 (총 {repo_count}개)")
            yield emit_status(2, done=True)

            yield emit_message(f"서비스 및 컨트롤러 생성을 시작합니다 ({total_files}개 파일)")

            for file_idx, (directory, file_name) in enumerate(file_names, 1):
                base_name = file_name.rsplit(".", 1)[0]
                yield emit_message(f"파일 변환 시작: {base_name} ({file_idx}/{total_files})")
                yield emit_message(f"경로: {directory}")

                yield emit_message("서비스 스켈레톤을 생성하고 있습니다")
                (
                    service_creation_info,
                    service_class_name,
                    exist_command_class,
                    command_class_list,
                ) = await self._step_service_skeleton(
                    entity_result_list,
                    directory,
                    file_name,
                    global_variables,
                    repository_list,
                )
                
                cmd_count = len(command_class_list)
                if cmd_count > 0:
                    yield emit_message(f"커맨드 클래스 {cmd_count}개를 생성하고 있습니다")
                for cmd_idx, command in enumerate(command_class_list, 1):
                    cmd_name, cmd_code = command["commandName"], command["commandCode"]
                    yield emit_message(f"커맨드 클래스 생성 완료: {cmd_name}.java ({cmd_idx}/{cmd_count})")
                    yield emit_data(
                        file_type="command_class",
                        file_name=f"{cmd_name}.java",
                        code=cmd_code,
                    )
                yield emit_message("서비스 스켈레톤 생성이 완료되었습니다")
                yield emit_status(3, done=True)

                yield emit_message("AI가 비즈니스 로직을 변환하고 있습니다")
                (
                    service_codes,
                    controller_name,
                    controller_code,
                ) = await self._step_service_and_controller(
                    service_creation_info,
                    service_class_name,
                    exist_command_class,
                    used_query_methods,
                    directory,
                    file_name,
                    sequence_methods,
                    base_name,
                )
                
                svc_count = len(service_codes)
                for svc_idx, service_code in enumerate(service_codes, 1):
                    yield emit_message(f"서비스 메서드 생성 완료 ({svc_idx}/{svc_count})")
                    yield emit_data(
                        file_type="service_class",
                        file_name=f"{service_class_name}.java",
                        code=service_code,
                    )
                
                yield emit_message(f"컨트롤러 생성 완료: {controller_name}.java")
                yield emit_data(
                    file_type="controller_class",
                    file_name=f"{controller_name}.java",
                    code=controller_code,
                )
                yield emit_message(f"파일 변환 완료: {base_name} ({file_idx}/{total_files})")
                yield emit_status(4, done=True)

            yield emit_message("설정 파일 및 메인 클래스를 생성하고 있습니다")
            
            config_results, main_code = await self._step_config_and_main()
            config_count = len(config_results) if config_results else 0
            
            for config_idx, (filename, content) in enumerate((config_results or {}).items(), 1):
                file_type = (
                    "pom"
                    if filename.endswith("pom.xml")
                    else ("properties" if filename.endswith(".properties") else "config")
                )
                yield emit_message(f"설정 파일 생성 완료: {filename} ({config_idx}/{config_count})")
                yield emit_data(file_type=file_type, file_name=filename, code=content)

            main_filename = f"{self.project_name.capitalize()}Application.java"
            yield emit_message(f"메인 클래스 생성 완료: {main_filename}")
            yield emit_data(
                file_type="main",
                file_name=main_filename,
                code=main_code,
            )
            yield emit_message(f"설정 파일 생성이 완료되었습니다 ({config_count}개 설정 파일 + 메인 클래스)")
            yield emit_status(5, done=True)

            yield emit_message(f"{target_name} 프레임워크 변환이 모두 완료되었습니다")
            yield emit_message(f"결과: 엔티티 {entity_count}개, 리포지토리 {repo_count}개, 서비스/컨트롤러 {total_files}개 파일, 설정 파일 {config_count + 1}개")

        except Exception as e:
            yield emit_error(f"프레임워크 변환 오류: {e.__class__.__name__}: {str(e)}")
            return

    async def _step_entity(self):
        return await EntityGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target
        ).generate()

    async def _step_repository(self):
        return await RepositoryGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target
        ).generate()

    async def _step_service_skeleton(
        self,
        entity_result_list,
        directory,
        file_name,
        global_variables,
        repository_list,
    ):
        return await ServiceSkeletonGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target
        ).generate(
            entity_result_list,
            directory,
            file_name,
            global_variables,
            repository_list,
        )

    async def _step_service_and_controller(
        self,
        service_creation_info,
        service_class_name,
        exist_command_class,
        used_query_methods,
        directory,
        file_name,
        sequence_methods,
        base_name,
    ):
        service_codes = []
        for svc in service_creation_info:
            svc_skeleton, cmd_var, proc_name = (
                svc["service_method_skeleton"],
                svc["command_class_variable"],
                svc["procedure_name"],
            )
            service_code = await start_service_preprocessing(
                svc_skeleton,
                cmd_var,
                proc_name,
                used_query_methods,
                directory,
                file_name,
                sequence_methods,
                self.project_name,
                self.user_id,
                self.api_key,
                self.locale,
                self.target,
            )
            service_codes.append(service_code)

        controller_name, controller_code = await ControllerGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target
        ).generate(
            base_name,
            service_class_name,
            exist_command_class,
            service_creation_info,
        )
        return service_codes, controller_name, controller_code

    async def _step_config_and_main(self):
        config_results = await ConfigFilesGenerator(
            self.project_name, self.user_id, self.target
        ).generate()
        main_code = await MainClassGenerator(
            self.project_name, self.user_id, self.target
        ).generate()
        return config_results, main_code
