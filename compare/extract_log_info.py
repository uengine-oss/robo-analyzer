import json
import os
from pathlib import Path
import time
import aiofiles
import base64
from decimal import Decimal
from datetime import datetime

# 현재 파일(capture_log.py)의 위치를 기준으로 경로 설정
CURRENT_DIR = Path(__file__).parent  # compare
PROJECT_ROOT = CURRENT_DIR.parent    # legacy-modernizer-back
LOGS_DIR = PROJECT_ROOT / 'logs'     # logs 디렉토리

# 로그 파일 경로
JAVA_LOG_PATH = LOGS_DIR / 'java_logs.jsonl'
PLSQL_LOG_PATH = LOGS_DIR / 'plsql_logs.jsonl'



async def compare_log_files(case_number: int) -> bool:
    """Then 로그 파일들을 비교하는 함수"""
    try:
        # Java와 PLSQL 로그 파일 읽기
        async with aiofiles.open(LOGS_DIR / f"extracted_then_java_case{case_number}.json", 'r') as f:
            java_logs = json.loads(await f.read())
        async with aiofiles.open(LOGS_DIR / f"extracted_then_plsql_case{case_number}.json", 'r') as f:
            plsql_logs = json.loads(await f.read())
            
        # 비교 결과 생성
        results = {}
        is_equal = True

        # 모든 키(operation_table)에 대해 비교
        all_keys = set(java_logs.keys()) | set(plsql_logs.keys())
        
        for key in all_keys:
            java_value = java_logs.get(key, {})
            plsql_value = plsql_logs.get(key, {})
            
            if java_value != plsql_value:
                is_equal = False
                results[key] = {
                    "status": "different",
                    "differences": compare_data(java_value, plsql_value)
                }
            else:
                results[key] = {"status": "identical"}

        # 결과 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        await save_json_to_file(results, f"compare_result_case{case_number}_{timestamp}.json")
        
        return is_equal
        
    except Exception as e:
        print(f"로그 비교 중 오류 발생: {str(e)}")
        return False



def compare_data(java: dict, plsql: dict) -> dict:
    """두 데이터 간의 차이점을 분석"""
    differences = {}
    
    for section in ["before", "after"]:
        java_section = java.get(section, {})
        plsql_section = plsql.get(section, {})
        
        if java_section != plsql_section:
            differences[section] = {
                field: {
                    "java": java_value,
                    "plsql": plsql_section.get(field)
                }
                for field, java_value in java_section.items()
                if java_value != plsql_section.get(field)
            }
                
    return differences



async def process_logs(log_type: str, case_number: int) -> dict:
    """Java와 PLSQL 로그 파일을 모두 처리하는 함수"""
    java_results = {}
    plsql_results = {}
    
    # 테이블별 카운터 초기화
    counters = {}
    
    # Java와 PLSQL 로그 순차 처리
    for db_type, log_path in [("java", JAVA_LOG_PATH), ("plsql", PLSQL_LOG_PATH)]:
        try:
            # 파일이 존재하고 내용이 있는지 확인
            if not log_path.exists() or log_path.stat().st_size == 0:
                print(f"{db_type} 로그 파일이 없거나 비어있습니다.")
                continue
                
            async with aiofiles.open(log_path, 'r', encoding='utf-8') as file:
                line_count = 0
                async for line in file:
                    try:
                        root_node = json.loads(line)
                        payload_node = root_node.get("payload", {})
                        
                        field_types = {
                            field["field"]: field.get("name", field["type"])
                            for field in root_node.get("schema", {}).get("fields", [{}])[0].get("fields", [])
                        }
                        
                        result = {
                            "fields": field_types,
                            "before": decode_data(payload_node.get("before"), field_types),
                            "after": decode_data(payload_node.get("after"), field_types),
                            "table": payload_node.get("source", {}).get("table", ""),
                            "operation": payload_node.get("op", "")
                        }
                        
                        # 테이블별 카운터 관리
                        table_name = result['table']
                        operation = result['operation']
                        counter_key = f"{operation}_{table_name}"
                        counters[counter_key] = counters.get(counter_key, 0) + 1
                        
                        # 고유한 키 생성 (operation_table_counter)
                        key = f"{result['operation']}_{result['table']}_{counters[counter_key]}"
                        
                        # Java와 PLSQL 결과를 각각의 딕셔너리에 저장
                        if db_type == "java":
                            java_results[key] = result
                        else:
                            plsql_results[key] = result
                            
                        line_count += 1
                        
                    except json.JSONDecodeError as e:
                        print(f"{db_type} 로그 파싱 오류 (라인 {line_count + 1}): {str(e)}")
                print(f"{db_type} 로그 처리 완료: {line_count} 라인")
            
            if line_count > 0:
                # 내용이 있었던 경우에만 파일 비우기
                async with aiofiles.open(log_path, 'w', encoding='utf-8') as file:
                    await file.write('')
                print(f"{db_type} 로그 파일 초기화 완료\n")
                
                # 내용이 있었던 경우에만 결과 파일 저장
                if db_type == "java" and java_results:
                    await save_json_to_file(java_results, f"extracted_{log_type}_java_case{case_number}.json")
                elif db_type == "plsql" and plsql_results:
                    await save_json_to_file(plsql_results, f"extracted_{log_type}_plsql_case{case_number}.json")
                
        except Exception as e:
            print(f"{db_type} 로그 처리 중 오류: {str(e)}")

    return {"java": java_results, "plsql": plsql_results}



async def extract_given_log(case_number: int) -> dict:
    """Given 로그 추출 (Java와 PLSQL 모두)"""
    time.sleep(15)
    return await process_logs("given", case_number)



async def extract_then_log(case_number: int) -> dict:
    """Then 로그 추출 (Java와 PLSQL 모두)"""
    time.sleep(15)
    return await process_logs("then", case_number)



def decode_data(data_node, field_types):
    """JSON 데이터를 필드 타입에 맞게 디코딩"""
    decoded = {}
    if data_node:
        for field_name, value in data_node.items():
            type_ = field_types.get(field_name)
            decoded[field_name] = decode_value(value, type_)
    return decoded



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