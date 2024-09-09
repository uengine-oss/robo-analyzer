import unittest
import os
import logging
import aiofiles

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 역할 : 전달받은 이름을 케밥 표기법을 전환하는 함수입니다,
# 매개변수 : 
#   - fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 케밥 표기법으로 전환된 프로젝트 이름
def convert_to_kebab_case(fileName):
    components = fileName.split('_')
    return '-'.join(x.lower() for x in components)


# 역할: Maven 프로젝트 설정 파일인 pom.xml을 생성하는 함수입니다.
# 매개변수: 
#   - fileName : 스토어드 프로시저 파일 이름
# 반환값: 없음
async def start_pomxml_processing(fileName):
    try:
    	# * 프로젝트의 이름을 케밥 케이스로 변경
        kebabCaseFileName = convert_to_kebab_case(fileName)


        # * pom.xml 파일의 내용을 문자열로 생성합니다.
        pom_xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.2</version>
        <relativePath/> <!-- lookup parent from repository -->
    </parent>
    <groupId>com.example</groupId>
    <artifactId>{kebabCaseFileName}</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>{kebabCaseFileName}</name>
    <description>{kebabCaseFileName} project for Spring Boot</description>
    <url/>
    <licenses>
        <license/>
    </licenses>
    <developers>
        <developer/>
    </developers>
    <scm>
        <connection/>
        <developerConnection/>
        <tag/>
        <url/>
    </scm>
    <properties>
        <java.version>17</java.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-rest</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
"""

        # * pom.xml 파일을 저장할 디렉토리를 생성합니다.
        pom_xml_directory = os.path.join('test', 'test_converting', 'converting_result', 'pom')  
        os.makedirs(pom_xml_directory, exist_ok=True)  

        # * 생성된 내용을 pom.xml 파일로 쓰기를 수행합니다.
        pom_xml_path = os.path.join(pom_xml_directory, "pom.xml")  
        async with aiofiles.open(pom_xml_path, 'w', encoding='utf-8') as file:  
            await file.write(pom_xml_content)  
            logging.info(f"\nSuccess Create Pom.xml\n")

    except Exception:
        logging.exception("Error occurred while create pom.xml")
        raise


# pom.xml를 생성하는 테스트 모듈
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_create_pom_file(self):
        await start_pomxml_processing("P_B_CAC120_CALC_SUIP_STD")


if __name__ == '__main__':
    unittest.main()