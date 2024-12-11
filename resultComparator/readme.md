-- debezium connect 삭제
curl -X DELETE http://localhost:8083/connectors/plsqldb-connector

-- debezium connect 목록 확인
curl -H "Accept:application/json" localhost:8083/connectors/

-- debezium connect 상태 확인
curl -s http://localhost:8083/connectors/plsqldb-connector/status

-- debezium connect 로그 확인
docker logs connect


-- kafka topic 목록 확인
winpty docker run --rm --name list-topics --link kafka:kafka quay.io/debezium/kafka:2.7 //kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092


-- 데모 사용자로 접속
docker exec -it oracle sqlplus c##debezium/dbz@//localhost:1521/plsqldb

-- 루트 사용자로 접속
docker exec -it oracle sqlplus sys/debezium@//localhost:1521/plsqldb as sysdba


-- 테이블 목록 확인
SELECT table_name FROM user_tables;


-- kafka topic 목록 확인
/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092


-- 자바 프로젝트 관련 db 확인
ALTER SESSION SET CONTAINER = JAVADB;
SHOW CON_NAME;
SELECT table_name FROM user_tables;
DESC C##DEBEZIUM.EMPLOYEES;
SELECT * FROM C##DEBEZIUM.EMPLOYEES;


-- 패키지, 패키지 바디 삭제
DROP PACKAGE TPX_TMF_SYNC_JOB_STATUS;
DROP PACKAGE COM_TYPE;
DROP PACKAGE BODY TPX_TMF_SYNC_JOB_STATUS;


-- TPX_ 접두어를 가진 모든 패키지와 패키지 바디 삭제
BEGIN
  FOR r IN (SELECT object_name, object_type 
            FROM user_objects 
            WHERE object_name LIKE 'TPX_%'
            AND object_type IN ('PACKAGE', 'PACKAGE BODY')) 
  LOOP
    EXECUTE IMMEDIATE 'DROP ' || r.object_type || ' ' || r.object_name;
  END LOOP;
END;
/



-- TPX_ 접두어를 가진 모든 객체의 상태 확인
SELECT OBJECT_NAME, OBJECT_TYPE, STATUS, LAST_DDL_TIME
FROM USER_OBJECTS 
WHERE OBJECT_NAME LIKE 'TPX_%'
ORDER BY OBJECT_TYPE, OBJECT_NAME;



-- TPX_ 접두어를 가진 객체들의 컴파일 에러 확인
SELECT NAME, TYPE, LINE, POSITION, TEXT, ATTRIBUTE
FROM USER_ERRORS 
WHERE NAME LIKE 'TPX_%'
ORDER BY NAME, SEQUENCE;

-- 패키지, 패키지 바디 상태 확인
SELECT object_name, object_type, status, created, last_ddl_time
FROM user_objects 
WHERE object_type IN ('PACKAGE', 'PACKAGE BODY')
ORDER BY object_name, object_type;