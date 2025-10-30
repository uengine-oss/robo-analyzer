import json
from typing import AsyncGenerator, Any
from .base_strategy import ConversionStrategy
from util.utility_tool import emit_message, emit_data, emit_error, emit_status

# 프레임워크 변환에 필요한 생성기/유틸 의존성들
from convert.create_entity import EntityGenerator
from convert.create_repository import RepositoryGenerator
from convert.create_service_skeleton import ServiceSkeletonGenerator
from convert.create_service_preprocessing import start_service_preprocessing
from convert.create_controller import ControllerGenerator
from convert.create_config_files import ConfigFilesGenerator
from convert.create_main import MainClassGenerator


class FrameworkConversionStrategy(ConversionStrategy):
    """프레임워크 변환 전략 (Spring Boot, FastAPI 등)"""
    
    def __init__(self, target_framework: str = "springboot"):
        self.target_framework = target_framework.lower()
    
    # 공통 컨텍스트 보관용 필드
    project_name: str | None = None
    user_id: str | None = None
    api_key: str | None = None
    locale: str | None = None
    target_lang: str | None = None
    
    async def convert(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """
        프레임워크 변환을 수행합니다.
        - Spring Boot의 경우: 엔티티 → 리포지토리 → 서비스 스켈레톤/바디 → 컨트롤러 → 설정/메인 생성
        - orchestrator는 공통 컨텍스트(유저/프로젝트/키/로케일 등)만 제공
        """
        if self.target_framework != "springboot":
            yield emit_error(f"Unsupported framework: {self.target_framework}")
            return

        # 공통 컨텍스트 저장 (한번만 설정)
        self.project_name = orchestrator.project_name
        self.user_id = orchestrator.user_id
        self.api_key = orchestrator.api_key
        self.locale = orchestrator.locale
        self.target_lang = orchestrator.target_lang

        try:
            # 프로젝트 이름 송신
            yield emit_data(file_type="project_name", project_name=self.project_name)

            # 엔티티/리포지토리는 프로젝트 단위로 1회 생성
            yield emit_message("Starting framework conversion")
            entity_result_list = await self._step_entity()
            yield emit_message("Generating Entities completed")
            yield emit_status(1, done=True)
            
            used_query_methods, global_variables, sequence_methods, repository_list = (
                await self._step_repository()
            )
            yield emit_message("Generating Repositories completed")
            yield emit_status(2, done=True)

            # 파일별로 서비스/컨트롤러 생성
            for folder_name, file_name in file_names:
                base_name = file_name.rsplit(".", 1)[0]
                yield emit_message(f"Processing {base_name}")

                service_creation_info, service_class_name, exist_command_class, command_class_list = (
                    await self._step_service_skeleton(
                        entity_result_list,
                        folder_name,
                        file_name,
                        global_variables,
                        repository_list,
                    )
                )
                yield emit_message(f"{base_name} - Service Skeleton")
                yield emit_status(3, done=True)

                await self._step_service_and_controller(
                    service_creation_info,
                    service_class_name,
                    exist_command_class,
                    used_query_methods,
                    folder_name,
                    file_name,
                    sequence_methods,
                    base_name,
                )
                yield emit_message(f"{base_name} - Service Body & Controller")
                yield emit_message(f"{base_name} - Completed")
                yield emit_status(4, done=True)

            yield emit_message("Generating Config Files")
            await self._step_config_and_main()
            yield emit_status(5, done=True)

        except Exception as e:
            yield emit_error(f"Framework conversion error: {e.__class__.__name__}: {str(e)}")
            return

    async def _step_entity(self):
        entity_result_list = await EntityGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target_lang
        ).generate()
        for entity in entity_result_list:
            entity_name, entity_code = entity['entityName'], entity['entityCode']
            yield emit_data(file_type="entity_class", file_name=f"{entity_name}.java", code=entity_code)
        return entity_result_list

    async def _step_repository(self):
        used_query_methods, global_variables, sequence_methods, repository_list = await RepositoryGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target_lang
        ).generate()
        for repo in repository_list:
            repo_name, repo_code = repo['repositoryName'], repo['code']
            yield emit_data(file_type="repository_class", file_name=f"{repo_name}.java", code=repo_code)
        return used_query_methods, global_variables, sequence_methods, repository_list

    async def _step_service_skeleton(self, entity_result_list, folder_name, file_name, global_variables, repository_list):
        service_creation_info, service_class_name, exist_command_class, command_class_list = (
            await ServiceSkeletonGenerator(
                self.project_name, self.user_id, self.api_key, self.locale, self.target_lang
            ).generate(entity_result_list, folder_name, file_name, global_variables, repository_list)
        )
        for command in command_class_list:
            cmd_name, cmd_code = command['commandName'], command['commandCode']
            yield emit_data(file_type="command_class", file_name=f"{cmd_name}.java", code=cmd_code)
        return service_creation_info, service_class_name, exist_command_class, command_class_list

    async def _step_service_and_controller(self, service_creation_info, service_class_name, exist_command_class,
                                           used_query_methods, folder_name, file_name, sequence_methods, base_name):

        for svc in service_creation_info:
            svc_skeleton, cmd_var, proc_name = (
                svc['service_method_skeleton'], svc['command_class_variable'], svc['procedure_name']
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
                self.target_lang,
            )
            yield emit_data(file_type="service_class", file_name=f"{service_class_name}.java", code=service_code)

        controller_name, controller_code = await ControllerGenerator(
            self.project_name, self.user_id, self.api_key, self.locale, self.target_lang
        ).generate(
            base_name, service_class_name, exist_command_class, service_creation_info
        )
        yield emit_data(file_type="controller_class", file_name=f"{controller_name}.java", code=controller_code)

    async def _step_config_and_main(self):
        config_results = await ConfigFilesGenerator(self.project_name, self.user_id, self.target_lang).generate()
        for filename, content in (config_results or {}).items():
            file_type = "pom" if filename.endswith("pom.xml") else ("properties" if filename.endswith(".properties") else "config")
            yield emit_data(file_type=file_type, file_name=filename, code=content)

        main_code = await MainClassGenerator(self.project_name, self.user_id, self.target_lang).generate()
        yield emit_data(file_type="main", file_name=f"{self.project_name.capitalize()}Application.java", code=main_code)
