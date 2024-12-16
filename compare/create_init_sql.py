from datetime import date
from pathlib import Path

async def generate_init_sql(table_names: list[str], package_names: list[str]) -> bool:
    """
    01_init_database_config.sql 파일을 생성하는 함수
    """
    try:
        # 테이블 생성 명령어 생성
        table_creation_plsql = "\n".join([f"@/opt/oracle/scripts/sql/ddl/{table}.sql" for table in table_names])
        package_creation_plsql = "\n".join([f"@/opt/oracle/scripts/sql/procedure/{package}.sql" for package in package_names])

        template = f'''SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_pdbs WHERE pdb_name = 'PLSQLDB';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE PLUGGABLE DATABASE plsqldb ADMIN USER pdbadmin IDENTIFIED BY dbz
    FILE_NAME_CONVERT = (''/opt/oracle/oradata/XE/'', ''/opt/oracle/oradata/XE/plsqldb/'')';
    EXECUTE IMMEDIATE 'ALTER PLUGGABLE DATABASE plsqldb OPEN';
  END IF;
END;
/

DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_pdbs WHERE pdb_name = 'JAVADB';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE PLUGGABLE DATABASE javadb ADMIN USER pdbadmin IDENTIFIED BY dbz
    FILE_NAME_CONVERT = (''/opt/oracle/oradata/XE/'', ''/opt/oracle/oradata/XE/javadb/'')';
    EXECUTE IMMEDIATE 'ALTER PLUGGABLE DATABASE javadb OPEN';
  END IF;
END;
/


DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'C##DEBEZIUM';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER c##debezium IDENTIFIED BY dbz CONTAINER=ALL';
    
    -- 기본 권한
    EXECUTE IMMEDIATE 'GRANT CONNECT, CREATE SESSION, SET CONTAINER TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$DATABASE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT FLASHBACK ANY TABLE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ANY TABLE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ANY TRANSACTION TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT LOGMINING TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT CREATE TABLE, LOCK ANY TABLE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT CREATE SEQUENCE TO c##debezium CONTAINER=ALL';

    -- 프로시저 생성 권한
    EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO c##debezium CONTAINER=ALL';

  END IF;
END;
/


ALTER SESSION SET CONTAINER = plsqldb;
ALTER SESSION SET CURRENT_SCHEMA = C##DEBEZIUM;
{table_creation_plsql}
{package_creation_plsql}
ALTER SESSION SET CONTAINER = CDB$ROOT;

DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'C##DEBEZIUM';
  IF v_count = 0 THEN

    -- LOGMNR 관련 권한
    EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON DBMS_LOGMNR TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON DBMS_LOGMNR_D TO c##debezium CONTAINER=ALL';
    
    -- V$ 뷰 접근 권한
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$LOG TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$LOG_HISTORY TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$LOGMNR_LOGS TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$LOGFILE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$ARCHIVED_LOG TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT SELECT ON V_$TRANSACTION TO c##debezium CONTAINER=ALL';
    
    -- QUOTA 설정
    EXECUTE IMMEDIATE 'ALTER USER c##debezium QUOTA UNLIMITED ON SYSTEM';
    EXECUTE IMMEDIATE 'ALTER USER c##debezium QUOTA UNLIMITED ON SYSAUX';
    
    -- 테이블스페이스 관련 권한
    EXECUTE IMMEDIATE 'GRANT CREATE TABLESPACE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT MANAGE TABLESPACE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT ALTER TABLESPACE TO c##debezium CONTAINER=ALL';
    EXECUTE IMMEDIATE 'GRANT DROP TABLESPACE TO c##debezium CONTAINER=ALL';
    
    -- 테이블 삭제 권한
    EXECUTE IMMEDIATE 'GRANT DROP ANY TABLE TO c##debezium CONTAINER=ALL';
  
  END IF;
END;
/


COMMIT;

EXIT;'''

        # 프로젝트 루트 경로 찾기
        current_dir = Path(__file__).parent.parent
        setup_dir = current_dir / 'setup'
        
        # setup 디렉토리가 없으면 생성
        setup_dir.mkdir(exist_ok=True)
        
        # 01_init_database_config.sql 파일 생성
        init_sql_path = setup_dir / '01_init_database_config.sql'
        with open(init_sql_path, 'w', encoding='utf-8') as f:
            f.write(template)
            
        print(f"01_init_database_config.sql 파일이 생성되었습니다: {init_sql_path}")
        return True
        
    except Exception as e:
        print(f"01_init_database_config.sql 파일 생성 중 오류 발생: {str(e)}")
        return False
    

def generate_insert_sql(table_fields: dict) -> list:
    insert_statements = []
    
    for table_name, fields in table_fields.items():
        columns = []
        values = []
        
        for field_name, field_info in fields.items():
            columns.append(field_name)
            
            # 필드 타입에 따른 값 포맷팅
            if field_info['type'].startswith(('VARCHAR2', 'CHAR')):
                values.append(f"'{field_info['value']}'")
            elif field_info['type'].startswith('NUMBER'):
                values.append(field_info['value'])
            elif field_info['type'].startswith('DATE'):
                values.append(f"TO_DATE('{field_info['value']}', 'YYYY-MM-DD')")
            else:
                values.append(f"'{field_info['value']}'")
        
        # INSERT 문 생성
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        insert_statements.append(insert_sql)
    
    return insert_statements



def extract_procedure_params(procedure: dict) -> dict:
    params = {}
    for var_name, var_info in procedure['variables'].items():
        value = var_info['value']
        param_type = var_info['type']
        
        # DATE 타입인 경우 date 객체로 변환
        if param_type == 'DATE':
            year, month, day = map(int, value.split('-'))
            value = date(year, month, day)
            
        # NUMBER 타입인 경우 정수로 변환
        elif param_type == 'NUMBER':
            value = int(value) if value.isdigit() else value
            
        # VARCHAR2, CHAR 등 문자열 타입은 그대로 사용
        else:
            value = value
            
        params[var_name] = value
    
    return params




