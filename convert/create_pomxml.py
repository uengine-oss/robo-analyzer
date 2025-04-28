import os
import logging
from util.exception import PomXmlCreationError, SaveFileError
from util.file_utils import save_file

POM_FILE_NAME = "pom.xml"


# 역할: Spring Boot 프로젝트의 필수 설정 파일인 pom.xml을 생성합니다.
#
# 매개변수: 
#   - user_id : 사용자 ID
#   - project_name : 프로젝트 이름
async def start_pomxml_processing(user_id:str, project_name:str) -> str:
    logging.info("pom.xml 생성을 시작합니다.")
    
    try:       
        # JPA POM 템플릿 생성
        pom_xml_template = f"""<?xml version="1.0" encoding="UTF-8"?>
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
    <artifactId>{project_name}</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>{project_name}</name>
    <description>{project_name} project for Spring Boot</description>
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
            <groupId>com.oracle.database.jdbc</groupId>
            <artifactId>ojdbc11</artifactId>
            <scope>runtime</scope>
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

        # POM 경로 설정
        pom_path = project_name

        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, pom_path)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, pom_path)


        # * pom.xml 파일 생성
        await save_file(
            content=pom_xml_template,
            filename=POM_FILE_NAME, 
            base_path=save_path
        )
        
        logging.info("Pom.xml이 생성되었습니다.\n")
        return pom_xml_template

    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"스프링부트의 Pom.xml 파일을 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise PomXmlCreationError(err_msg)