import asyncio
import os
import sys

sys.path.append('/Users/jhyg/Desktop/legacy-modernizer/legacy-modernizer-back')
from convert.create_service_skeleton import execute_maven_commands

async def test_execute_maven_commands():
    # 환경 변수 설정 (필요한 경우)
    os.environ['DOCKER_COMPOSE_CONTEXT'] = '/path/to/docker/context'

    # pom_directory 설정
    pom_directory = '/path/to/your/pom/directory'

    # execute_maven_commands 함수 호출
    await execute_maven_commands(pom_directory)

# asyncio.run을 사용하여 비동기 함수 실행
if __name__ == "__main__":
    asyncio.run(test_execute_maven_commands())