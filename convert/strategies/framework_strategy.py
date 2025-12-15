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
            yield emit_error(f"Unsupported target: {self.target}")
            return
        
        if self.target == "python":
            yield emit_error("Python FastAPI conversion is not yet supported")
            return

        # 공통 컨텍스트 저장
        self.project_name = orchestrator.project_name
        self.user_id = orchestrator.user_id
        self.api_key = orchestrator.api_key
        self.locale = orchestrator.locale

        try:
            # 프로젝트 이름 송신
            yield emit_data(file_type="project_name", project_name=self.project_name)

            # 엔티티/리포지토리는 프로젝트 단위로 1회 생성
            yield emit_message("Starting framework conversion")
            entity_result_list = await self._step_entity()
            for entity in entity_result_list:
                entity_name, entity_code = entity["entityName"], entity["entityCode"]
                yield emit_data(
                    file_type="entity_class",
                    file_name=f"{entity_name}.java",
                    code=entity_code,
                )
            yield emit_message("Generating Entities completed")
            yield emit_status(1, done=True)

            (
                used_query_methods,
                global_variables,
                sequence_methods,
                repository_list,
            ) = await self._step_repository()
            for repo in repository_list:
                repo_name, repo_code = repo["repositoryName"], repo["code"]
                yield emit_data(
                    file_type="repository_class",
                    file_name=f"{repo_name}.java",
                    code=repo_code,
                )
            yield emit_message("Generating Repositories completed")
            yield emit_status(2, done=True)

            # 파일별로 서비스/컨트롤러 생성
            for folder_name, file_name in file_names:
                base_name = file_name.rsplit(".", 1)[0]
                yield emit_message(f"Processing {base_name}")

                (
                    service_creation_info,
                    service_class_name,
                    exist_command_class,
                    command_class_list,
                ) = await self._step_service_skeleton(
                    entity_result_list,
                    folder_name,
                    file_name,
                    global_variables,
                    repository_list,
                )
                for command in command_class_list:
                    cmd_name, cmd_code = command["commandName"], command["commandCode"]
                    yield emit_data(
                        file_type="command_class",
                        file_name=f"{cmd_name}.java",
                        code=cmd_code,
                    )
                yield emit_message(f"{base_name} - Service Skeleton")
                yield emit_status(3, done=True)

                (
                    service_codes,
                    controller_name,
                    controller_code,
                ) = await self._step_service_and_controller(
                    service_creation_info,
                    service_class_name,
                    exist_command_class,
                    used_query_methods,
                    folder_name,
                    file_name,
                    sequence_methods,
                    base_name,
                )
                for service_code in service_codes:
                    yield emit_data(
                        file_type="service_class",
                        file_name=f"{service_class_name}.java",
                        code=service_code,
                    )
                yield emit_data(
                    file_type="controller_class",
                    file_name=f"{controller_name}.java",
                    code=controller_code,
                )
                yield emit_message(f"{base_name} - Service Body & Controller")
                yield emit_message(f"{base_name} - Completed")
                yield emit_status(4, done=True)

            yield emit_message("Generating Config Files")
            config_results, main_code = await self._step_config_and_main()
            for filename, content in (config_results or {}).items():
                file_type = (
                    "pom"
                    if filename.endswith("pom.xml")
                    else ("properties" if filename.endswith(".properties") else "config")
                )
                yield emit_data(file_type=file_type, file_name=filename, code=content)

            yield emit_data(
                file_type="main",
                file_name=f"{self.project_name.capitalize()}Application.java",
                code=main_code,
            )
            yield emit_status(5, done=True)

        except Exception as e:
            yield emit_error(f"Framework conversion error: {e.__class__.__name__}: {str(e)}")
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
        folder_name,
        file_name,
        global_variables,
        repository_list,
    ):
        return await ServiceSkeletonGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target
        ).generate(
            entity_result_list,
            folder_name,
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
        folder_name,
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
                folder_name,
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
