import asyncio
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from compare.result_compare import execute_maven_commands

async def test_execute_maven_commands():
    # # 환경 변수 설정 (필요한 경우)
    # os.environ['DOCKER_COMPOSE_CONTEXT'] = '/path/to/docker/context'

    # # pom_directory 설정
    # pom_directory = '/path/to/your/pom/directory'

    # 테스트할 클래스 이름 리스트 정의
    test_classes = [
        "TpxUpdateSalaryDeduct100AmountTest",
        "TpjSalaryDeduct100AmountTest"
    ]

    # execute_maven_commands 함수 호출
    await execute_maven_commands(test_classes)

# asyncio.run을 사용하여 비동기 함수 실행
if __name__ == "__main__":
    asyncio.run(test_execute_maven_commands())