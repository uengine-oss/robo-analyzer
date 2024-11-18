import os
import logging
import aiofiles
from util.exception import PomXmlCreationError


# 역할: Maven 프로젝트 설정 파일인 pom.xml을 생성하는 함수입니다.
# 매개변수: 없음
# 반환값: 없음
async def start_pomxml_processing():
    
    logging.info("pom.xml 생성을 시작합니다.")
    
    try:        
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
	<artifactId>demo</artifactId>
	<version>0.0.1-SNAPSHOT</version>
	<name>demo</name>
	<description>demo project for Spring Boot</description>
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
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            pom_xml_directory = os.path.join(base_directory, 'java', 'demo')
        else:
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            pom_xml_directory = os.path.join(current_dir, 'target', 'java', 'demo')
        os.makedirs(pom_xml_directory, exist_ok=True)


        # * 생성된 내용을 pom.xml 파일로 쓰기를 수행합니다.
        pom_xml_path = os.path.join(pom_xml_directory, "pom.xml")  
        async with aiofiles.open(pom_xml_path, 'w', encoding='utf-8') as file:  
            await file.write(pom_xml_content)  
            logging.info(f"Pom.xml이 생성되었습니다.\n")

    except Exception:
        err_msg = "스프링부트의 Pom.xml 파일을 생성하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise PomXmlCreationError(err_msg)