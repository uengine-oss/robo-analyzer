from pathlib import Path
import time
import aiofiles
import oracledb  # cx_Oracle 대신 사용

# DB 연결 정보
DB_CONFIG = {
    'user': 'c##debezium',
    'password': 'dbz',
    'dsn': 'localhost:1521/plsqldb'  
}

async def execute_sql(sql_content: list) -> bool:
    """SQL 실행"""
    connection = oracledb.connect(**DB_CONFIG)
    cursor = connection.cursor()
    
    try:
        for statement in sql_content:
            if statement.strip():
                cursor.execute(statement)
                    
        connection.commit()
        return True
        
    except Exception as e:
        print(f"SQL 실행 중 오류: {str(e)}")
        return False
    finally:
        cursor.close()
        connection.close()



async def execute_plsql(plsql_name: str, params: dict) -> bool:
    """PLSQL 프로시저 실행"""
    connection = oracledb.connect(**DB_CONFIG)
    cursor = connection.cursor()
    print(params)
    
    try:
        cursor.callproc(plsql_name, keywordParameters=params)
        connection.commit()
        return True
        
    except Exception as e:
        print(f"PLSQL 실행 중 오류: {str(e)}")
        return False
    finally:
        cursor.close()
        connection.close()