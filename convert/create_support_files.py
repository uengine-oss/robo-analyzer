import os
import logging
from prompt.convert_support_prompt import convert_xml_mapper
from util.exception import TemplateGenerationError
from util.file_utils import save_file

MAPPER_PATH = 'java/demo/src/main/resources/mapper'

# 역할 : MyBatis Mapper XML 파일을 생성합니다.
#
# 매개변수 : 
#   - entity_code : 엔티티 정보를 담은 딕셔너리
#   - query_methods : 리포지토리 메서드 정보를 담은 딕셔너리
#   - sequence_data : 시퀀스 정보를 담은 딕셔너리
async def start_mybatis_mapper_processing(entity_infos: dict, all_query_methods: dict, sequence_data: str) -> None:
    try:

        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), MAPPER_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', MAPPER_PATH)


        # * 각 테이블별로 Mapper XML 생성
        for entity_name, entity_code in entity_infos.items():
            analysis_mapper = await convert_xml_mapper(entity_name, entity_code, all_query_methods[entity_name], sequence_data)
            entity_name = analysis_mapper['mapperName']
            entity_code = analysis_mapper['code']

            # * 파일 저장
            await save_file(
                content=entity_code,
                filename=f"{entity_name}Mapper.xml",
                base_path=save_path
            )
            
    except Exception as e:
        err_msg = f"MyBatis Mapper 생성 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise TemplateGenerationError(err_msg)