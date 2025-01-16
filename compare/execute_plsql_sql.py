import logging
import oracledb

from util.exception import ExecutePlsqlError, ExecuteSqlError

# DB 연결 정보
DB_CONFIGS = {
    'plsqldb': {
        'user': 'c##debezium',
        'password': 'dbz',
        'dsn': 'localhost:1521/plsqldb'
    },
    'javadb': {
        'user': 'c##debezium',
        'password': 'dbz',
        'dsn': 'localhost:1521/javadb'
    }
}

# 역할 : SQL 실행 함수
#
# 매개변수 : 
#   - sql_content : SQL 명령어 리스트
#   - orm_type : 사용할 ORM 유형
#   - is_delete : 테스트 데이터 삭제 여부
#
# 반환값 : 
#   - bool : SQL 실행 성공 여부
async def execute_sql(sql_content: list, orm_type: str = 'jpa', is_delete: bool = False) -> bool:
    db_list = []
    
    # * orm_type에 따라 실행할 데이터베이스 결정
    if is_delete and orm_type == 'mybatis':
        db_list = ['plsqldb', 'javadb']
    else: 
        db_list = ['plsqldb']
    
    try:
        # * 선택된 모든 데이터베이스에 대해 SQL 실행
        for db_name in db_list:
            with oracledb.connect(**DB_CONFIGS[db_name]) as connection:
                with connection.cursor() as cursor:
                    for statement in sql_content:
                        if statement.strip():
                            cursor.execute(statement)
                    connection.commit()
        return True
        
    except Exception as e:
        err_msg = f"SQL 실행 중 오류: {str(e)}"
        logging.error(err_msg)
        raise ExecuteSqlError(err_msg)
    

# 역할 : PLSQL 프로시저 실행 함수
#
# 매개변수 : 
#   - plsql_name : PLSQL 프로시저 이름
#   - params : PLSQL 프로시저 매개변수 딕셔너리
#
# 반환값 : 
#   - bool : PLSQL 프로시저 실행 성공 여부
async def execute_plsql(plsql_name: str, params: dict, db_name: str = 'plsqldb') -> bool:
    
    try:
        # * DB 연결
        connection = oracledb.connect(**DB_CONFIGS[db_name])
        cursor = connection.cursor()

        # * PLSQL 프로시저 실행
        cursor.callproc(plsql_name, keywordParameters=params)
        connection.commit()
        return True
        
    except Exception as e:
        err_msg = f"PLSQL 실행 중 오류: {str(e)}"
        logging.error(err_msg)
        raise ExecutePlsqlError(err_msg)
    finally:
        cursor.close()
        connection.close()