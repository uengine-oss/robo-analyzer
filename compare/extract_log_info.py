import asyncio
import json
import logging
import os
from pathlib import Path
import time
import aiofiles
import base64
from decimal import Decimal
from datetime import date, datetime

from util.exception import CompareResultError, DecodeLogError, ExtractLogError, LogStabilizationError, ProcessResultError, SaveFileError
from collections import OrderedDict

# 현재 파일(capture_log.py)의 위치를 기준으로 경로 설정
CURRENT_DIR = Path(__file__).parent  
PROJECT_ROOT = CURRENT_DIR.parent    
LOGS_DIR = PROJECT_ROOT / 'logs'     

# 로그 파일 경로
JAVA_LOG_PATH = LOGS_DIR / 'java_logs.jsonl'
PLSQL_LOG_PATH = LOGS_DIR / 'plsql_logs.jsonl'


# 역할 : 결과 비교 함수
#
# 매개변수 : 
#   - case_number : 케이스 번호
#
# 반환값 : 
#   - dict : 결과 딕셔너리
async def compare_then_results(case_number: int):
    try:
        # * 파일 읽기
        async with aiofiles.open(LOGS_DIR / f"result_plsql_given_when_then_case{case_number}.json", 'r', encoding='utf-8') as f1, \
                  aiofiles.open(LOGS_DIR / f"result_java_given_when_then_case{case_number}.json", 'r', encoding='utf-8') as f2:
            plsql_data = json.loads(await f1.read())
            java_data = json.loads(await f2.read())

        def normalize_data(data: dict) -> dict:
            """데이터 정규화: ID 제외, 날짜에서 시간 제거, 키 이름 정규화"""
            return {
                k.replace('_', '').upper(): (
                    v.split()[0] if isinstance(v, str) and ' ' in v else v
                )
                for k, v in data.items() 
                if k.upper() != 'ID'
            }

        # * then 데이터 정규화
        plsql_then = {
            f"{item['operation']}_{item['table']}": normalize_data(item['data'])
            for item in plsql_data["then"]
        }

        java_then = {
            f"{item['operation']}_{item['table']}": normalize_data(item['data'])
            for item in java_data["then"]
        }

        # * 차이점 분석
        differences = {}
        has_differences = False

        for op_table in set(plsql_then.keys()) | set(java_then.keys()):
            plsql_values = plsql_then.get(op_table, {})
            java_values = java_then.get(op_table, {})

            if plsql_values != java_values:
                has_differences = True
                diff_fields = {
                    field: {
                        "java": str(java_values.get(field)),
                        "plsql": str(plsql_values.get(field))
                    }
                    for field in set(plsql_values.keys()) | set(java_values.keys())
                    if plsql_values.get(field) != java_values.get(field)
                }

                if diff_fields:
                    differences[op_table] = {
                        "status": "different",
                        "procedure_name": plsql_data["when"]["procedure"].split('.')[-1],
                        "differences": {
                            "after": diff_fields
                        }
                    }

                    
        # * 결과 생성
        result = (differences if has_differences else {
            "status": "identical",
            "message": "모든 결과가 동일합니다.",
            "procedure_name": plsql_data["when"]["procedure"].split('.')[-1]
        })
        
        logging.info(f"Case {case_number}: {'차이점이 발견되었습니다.' if has_differences else '모든 결과가 동일합니다.'}")


        # * 결과를 파일로 저장
        async with aiofiles.open(LOGS_DIR / f"compare_result_case{case_number}.json", 'w', encoding='utf-8') as f:
            await f.write(json.dumps(result, indent=4, ensure_ascii=False))

        return result

    except Exception as e:
        err_msg = f"결과 비교 로그 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise CompareResultError(err_msg)
    


# 역할 : Java Given-When-Then 로그 추출 함수
#
# 매개변수 : 
#   - case_number : 케이스 번호
#
# 반환값 : 
#   - dict : 추출된 로그 딕셔너리   
async def extract_java_given_when_then(case_number: int):
    try:
        # * 로그 파일 안정화 대기
        await wait_for_log_stabilization(JAVA_LOG_PATH)


        # * 파일 읽기
        async with aiofiles.open(LOGS_DIR / f"result_plsql_given_when_then_case{case_number}.json", 'r', encoding='utf-8') as f1, \
                  aiofiles.open(JAVA_LOG_PATH, encoding='utf-8') as f2:
            plsql_data = json.loads(await f1.read())
            java_entries = [json.loads(line) for line in (await f2.read()).split('\n') if line.strip()]


        # * 데이터 정규화
        def normalize(data: dict) -> dict:
            """데이터 정규화: ID 제외, 키 변환, 값 처리를 한번에 수행"""
            return {
                k.replace('_', '').upper(): (
                    int(v) if str(v).isdigit() else 
                    v.split(' ')[0] if isinstance(v, str) and ' ' in v and ':' in v 
                    else v
                )
                for k, v in data.items() 
                if k.upper() != 'ID'
            }


        # * PLSQL given 데이터 정규화
        given_keys = {entry['table']: normalize(entry['data']) for entry in plsql_data['given']}
        
        given_data = []
        then_data = []
        

        # * 데이터 처리 및 분류
        for entry in (e for e in java_entries if e['payload'].get('after')):
            table = entry['payload']['source']['table']
            type_info = {f['field']: f.get('name') for f in entry['schema']['fields'][1]['fields']}
            
            # * 데이터 디코딩 및 처리
            decoded_data = {
                k: (int(v) if str(v).isdigit() else 
                    v.split(' ')[0] if isinstance(v, str) and ' ' in v and ':' in v 
                    else v)
                for k, v in ((key, decode_value(value, type_info.get(key)))
                           for key, value in entry['payload']['after'].items())
            }
            
            # * 데이터 분류
            if table in given_keys and normalize(decoded_data) == given_keys[table]:
                given_data.append({
                    "operation": entry['payload']['op'],
                    "table": table,
                    "data": decoded_data
                })
            else:
                then_data.append({
                    "operation": entry['payload']['op'],
                    "table": table,
                    "data": decoded_data
                })


        # * OrderedDict로 순서 보장
        result = OrderedDict([
            ("given", given_data),
            ("when", plsql_data["when"]),
            ("then", then_data)
        ])


        # * 결과를 파일로 저장
        await save_json_to_file(result, f"result_java_given_when_then_case{case_number}.json")
        return result
    
    except (SaveFileError, DecodeLogError, LogStabilizationError):
        raise
    except Exception as e:
        err_msg = f"Java Given-When-Then 로그 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ExtractLogError(err_msg)
    

# 역할 : PLSQL Given-When-Then 로그 추출 함수
#
# 매개변수 : 
#   - case_number : 케이스 번호
#   - procedure : 프로시저 정보
#   - params : 프로시저 매개변수
#   - table_fields : 테이블 필드 정보
#
# 반환값 : 
#   - dict : 추출된 로그 딕셔너리
async def generate_given_when_then(case_number: int, procedure: dict, params: dict, table_fields: dict):
    try:
        # * 로그 파일 안정화 대기
        await wait_for_log_stabilization(PLSQL_LOG_PATH)

    
        # * GIVEN 생성 - 숫자는 숫자 타입으로 변환
        given_data = [
            {
                "operation": "c",
                "table": table_name,
                "data": {
                    field_name: int(field_info["value"]) if field_info["value"].isdigit() else field_info["value"]
                    for field_name, field_info in fields.items()
                }
            }
            for table_name, fields in table_fields.items()
        ]


        # * date 객체를 문자열로 변환하는 처리를 추가
        formatted_params = {}
        for key, value in params.items():
            if isinstance(value, date):  # date 객체인 경우
                formatted_params[key] = value.strftime('%Y-%m-%d')
            else:
                formatted_params[key] = value

        when_data = {
            "procedure": f"{procedure['object_name']}.{procedure['procedure_name']}",
            "parameters": formatted_params
        }


        # * THEN 생성 - 모든 JSON 라인 처리
        async with aiofiles.open(PLSQL_LOG_PATH, encoding='utf-8') as f:
            content = (await f.read()).replace('\x00', '')
            
            json_entries = [json.loads(line) for line in content.split('\n') if line.strip()]
            
            then_list = [
                {
                    "operation": entry["payload"]["op"],
                    "table": entry["payload"]["source"]["table"],
                    "data": {
                        key: decode_value(value, "io.debezium.time.Timestamp" if key == "PAY_DATE" else None)
                        for key, value in entry["payload"]["after"].items()
                    }
                }
                for entry in json_entries
                if entry["payload"].get("after")
            ]


            # * 결과를 파일로 저장
            result = {"given": given_data, "when": when_data, "then": then_list}
            await save_json_to_file(result, f"result_plsql_given_when_then_case{case_number}.json")
            return result

    except (SaveFileError, DecodeLogError, LogStabilizationError):
        raise
    except Exception as e:
        err_msg = f"Given-When-Then 로그 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ExtractLogError(err_msg)
    
    
# 역할 : 개별 값을 해당 타입에 맞게 디코딩
#
# 매개변수 : 
#   - value : 디코딩할 값
#   - type_ : 타입
#
# 반환값 : 
#   - str : 디코딩된 값
def decode_value(value, type_):
    if value is None:
        return value

    try:
        if type_ == "org.apache.kafka.connect.data.Decimal":
            bytes_ = base64.b64decode(value)
            return str(int.from_bytes(bytes_, byteorder='big'))

        if type_ == "io.debezium.data.VariableScaleDecimal":
            scale = value.get("scale", 0)
            scale_bytes = base64.b64decode(value.get("value", ""))
            unscaled_value = int.from_bytes(scale_bytes, byteorder='big')
            return str(Decimal(unscaled_value) / (10 ** scale))

        if type_ == "io.debezium.time.Timestamp":
            epoch_millis = int(value)
            return datetime.fromtimestamp(epoch_millis / 1000).strftime('%Y-%m-%d %H:%M:%S')

        return value
    
    except Exception as e:
        err_msg = f"로그 데이터 디코딩 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise DecodeLogError(err_msg)


# TODO : utils로 이동
async def save_json_to_file(data, file_name):
    """JSON 데이터를 파일로 저장"""
    logs_dir = LOGS_DIR
    logs_dir.mkdir(exist_ok=True)
    
    file_path = logs_dir / file_name
    print(f"파일 저장 경로: {file_path}")
    
    try:
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=4, ensure_ascii=False))
        print(f"파일 저장 완료: {file_name}")
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {str(e)}")


# 역할 : 지정된 로그 파일들이 안정화될 때까지 대기한 후 비웁니다.
#
# 매개변수 : 
#   - log_types : 비울 로그 파일 경로들
#
# 반환값 : 
#   - bool : 로그 파일 비우기 성공 여부
async def clear_log_files(*log_types: str):

    try:
        # * 로그 타입에 따른 파일 경로 매핑
        log_paths = []
        for log_type in log_types:
            if log_type.lower() == 'java':
                log_paths.append(JAVA_LOG_PATH)
            elif log_type.lower() == 'plsql':
                log_paths.append(PLSQL_LOG_PATH)
        

        # * 유효한 로그 타입이 아닌 경우 예외 발생
        if not log_paths:
            raise ValueError("유효한 로그 타입을 지정해주세요 ('java' 또는 'plsql')")


        # * 모든 파일이 안정화될 때까지 대기 (필수)
        await asyncio.gather(*[
            wait_for_log_stabilization(file_path)
            for file_path in log_paths
        ])
        logging.info("모든 로그 파일이 안정화되었습니다.")


        # * 안정화된 파일들 비우기
        async with asyncio.TaskGroup() as tg:
            for file_path in log_paths:
                if await aiofiles.os.path.exists(file_path):
                    async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                        await f.write('')
                    logging.info(f"로그 파일 비우기 완료: {file_path}")
                else:
                    logging.warning(f"로그 파일이 존재하지 않습니다: {file_path}")
            
    except LogStabilizationError:
        raise
    except Exception as e:
        err_msg = f"로그 파일 비우기 실패: {str(e)}"
        logging.error(err_msg)
        raise ExtractLogError(err_msg)
    

# 역할 : 로그 파일의 크기 변화를 모니터링하여 안정화될 때까지 대기합니다.
#
# 매개변수 : 
#   - log_path : 모니터링할 로그 파일 경로
#   - timeout : 전체 제한 시간 (3분)
#   - check_interval : 파일 체크 주기 (0.5초)
#   - stable_duration : 안정화 판단 기준 시간 (5초 = 0.5초 * 10회)
#
# 반환값 : 
#   - bool : 로그 파일 안정화 대기 성공 여부
async def wait_for_log_stabilization(log_path: Path, timeout: int = 60, check_interval: float = 2, stable_duration: int = 10):
    
    try:
        start_time = time.time()
        last_size = -1
        stable_count = 0
        
        while True:
            # * 파일 존재 여부 확인
            if not os.path.exists(log_path):
                logging.warning(f"로그 파일이 아직 생성되지 않았습니다: {log_path}")
                await asyncio.sleep(check_interval)
                continue

            # * 현재 파일 크기 확인
            current_size = os.path.getsize(log_path)
            
            # * 파일 크기 변화 감지
            if current_size == last_size:
                stable_count += 1
                if stable_count >= stable_duration:
                    logging.info(f"로그 파일이 안정화되었습니다. (크기: {current_size} bytes, 소요시간: {time.time() - start_time:.1f}초)")
                    break
            else:
                stable_count = 0
                last_size = current_size
                logging.debug(f"로그 파일 크기 변화 감지: {current_size} bytes")

            # * 타임아웃 체크
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"로그 파일 안정화 대기 시간이 초과되었습니다. "
                    f"(제한 시간: {timeout}초, 현재 파일 크기: {current_size} bytes)"
                )

            await asyncio.sleep(check_interval)
    
    except Exception as e:
        err_msg = f"로그 파일 안정화 대기 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LogStabilizationError(err_msg)

