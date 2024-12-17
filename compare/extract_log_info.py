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

from util.exception import ProcessResultError
from collections import OrderedDict

# 현재 파일(capture_log.py)의 위치를 기준으로 경로 설정
CURRENT_DIR = Path(__file__).parent  # compare
PROJECT_ROOT = CURRENT_DIR.parent    # legacy-modernizer-back
LOGS_DIR = PROJECT_ROOT / 'logs'     # logs 디렉토리

# 로그 파일 경로
JAVA_LOG_PATH = LOGS_DIR / 'java_logs.jsonl'
PLSQL_LOG_PATH = LOGS_DIR / 'plsql_logs.jsonl'



async def compare_then_results(case_number: int):
    try:
        # 파일 읽기
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

        # then 데이터 정규화
        plsql_then = {
            f"{item['operation']}_{item['table']}": normalize_data(item['data'])
            for item in plsql_data["then"]
        }

        java_then = {
            f"{item['operation']}_{item['table']}": normalize_data(item['data'])
            for item in java_data["then"]
        }

        # 차이점 분석
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

        # 결과 생성
        result = (differences if has_differences else {
            "status": "identical",
            "message": "모든 결과가 동일합니다.",
            "procedure_name": plsql_data["when"]["procedure"].split('.')[-1]
        })

        logging.info(f"Case {case_number}: {'차이점이 발견되었습니다.' if has_differences else '모든 결과가 동일합니다.'}")

        # 결과를 파일로 저장
        async with aiofiles.open(LOGS_DIR / f"compare_result_case{case_number}.json", 'w', encoding='utf-8') as f:
            await f.write(json.dumps(result, indent=4, ensure_ascii=False))

        return result

    except Exception as e:
        logging.error(f"결과 비교 중 오류가 발생했습니다: {str(e)}", exc_info=True)
        raise ProcessResultError(f"결과 비교 중 오류가 발생했습니다: {str(e)}")
    


async def extract_java_given_when_then(case_number: int):
    try:
        time.sleep(10)

        # 파일 읽기
        async with aiofiles.open(LOGS_DIR / f"result_plsql_given_when_then_case{case_number}.json", 'r', encoding='utf-8') as f1, \
                  aiofiles.open(JAVA_LOG_PATH, encoding='utf-8') as f2:
            plsql_data = json.loads(await f1.read())
            java_entries = [json.loads(line) for line in (await f2.read()).split('\n') if line.strip()]

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

        # PLSQL given 데이터 정규화
        given_keys = {entry['table']: normalize(entry['data']) for entry in plsql_data['given']}
        
        given_data = []
        then_data = []
        
        # 데이터 처리 및 분류
        for entry in (e for e in java_entries if e['payload'].get('after')):
            table = entry['payload']['source']['table']
            type_info = {f['field']: f.get('name') for f in entry['schema']['fields'][1]['fields']}
            
            # 데이터 디코딩 및 처리
            decoded_data = {
                k: (int(v) if str(v).isdigit() else 
                    v.split(' ')[0] if isinstance(v, str) and ' ' in v and ':' in v 
                    else v)
                for k, v in ((key, decode_value(value, type_info.get(key)))
                           for key, value in entry['payload']['after'].items())
            }
            
            # 데이터 분류
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

        # OrderedDict로 순서 보장
        result = OrderedDict([
            ("given", given_data),
            ("when", plsql_data["when"]),
            ("then", then_data)
        ])

        await save_json_to_file(result, f"result_java_given_when_then_case{case_number}.json")
        return result

    except Exception as e:
        logging.error(f"Java Given-When-Then 로그 생성 중 오류가 발생했습니다: {str(e)}", exc_info=True)
        raise ProcessResultError(f"Java Given-When-Then 로그 생성 중 오류가 발생했습니다: {str(e)}")
    


async def generate_given_when_then(case_number: int, procedure: dict, params: dict, table_fields: dict):
    try:
        time.sleep(10)

        # GIVEN 생성 - 숫자는 숫자 타입으로 변환
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


        # date 객체를 문자열로 변환하는 처리를 추가
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


        # THEN 생성 - 모든 JSON 라인 처리
        async with aiofiles.open(PLSQL_LOG_PATH, encoding='utf-8') as f:
            content = await f.read()
            
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

            result = {"given": given_data, "when": when_data, "then": then_list}
            await save_json_to_file(result, f"result_plsql_given_when_then_case{case_number}.json")
            return result


    except Exception as e:
        err_msg = f"Given-When-Then 로그 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg, exc_info=True)
        raise ProcessResultError(err_msg)
    
    

def decode_value(value, type_):
    """개별 값을 해당 타입에 맞게 디코딩"""
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
    except Exception:
        return value



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


async def clear_log_file(log_type: str):

    try:
            
        # PLSQL 로그 타입일 경우 딜레이
        if log_type == 'plsql':
            time.sleep(10)
        
        # 두 파일 동시 처리
        async with aiofiles.open(PLSQL_LOG_PATH, 'w', encoding='utf-8') as f1, \
                  aiofiles.open(JAVA_LOG_PATH, 'w', encoding='utf-8') as f2:
            await asyncio.gather(
                f1.write(''),
                f2.write('')
            )
            
        print(f"{log_type} 로그 파일 비우기 완료")
            
    except Exception as e:
        logging.error(f"{log_type} 로그 파일 비우기 실패: {str(e)}")
        raise ProcessResultError(f"{log_type} 로그 파일 비우기 실패")