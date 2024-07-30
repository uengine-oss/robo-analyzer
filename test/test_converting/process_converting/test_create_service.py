import asyncio
import json
import logging
import os
import re
import sys
import unittest
import aiofiles
import tiktoken

logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from test_converting.converting_prompt.service_prompt import convert_code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from cypher.neo4j_connection import Neo4jConnection


# * 인코더 설정 및 파일 이름 초기화
encoder = tiktoken.get_encoding("cl100k_base")
fileName = None


# 역할: 주어진 스토어드 프로시저 코드의 토큰의 개수를 계산하는 함수입니다.
# 매개변수: 
#      - code - 토큰을 계산할 스토어드 프로시저
# 반환값: 계산된 토큰의 수
def count_tokens_in_text(code):
    
    if code is None: return 0

    try:
        # * 코드를 토큰화하고 토큰의 개수를 반환합니다.
        tokens = encoder.encode(code)
        return len(tokens)
    except Exception:
        logging.exception("Unexpected error occurred during token counting(converting)")
        raise


# 역할: 주어진 범위에서 startLine과 endLine을 추출해서 스토어드 프로시저 코드를 잘라내는 함수입니다.
# 매개변수: 
#     - code : 스토어드 프로시저 코드
#     - context_range : 잘라낼 범위를 나타내는 딕셔너리의 리스트
# 반환값: 범위에 맞게 추출된 스토어드 프로시저 코드.
def extract_code_within_range(code, context_range):
    try:
        if not code or not context_range:
            return ""

        # * context_range에서 가장 작은 시작 라인과 가장 큰 끝 라인을 찾습니다.
        start_line = min(range_item['startLine'] for range_item in context_range)
        end_line = max(range_item['endLine'] for range_item in context_range)


        # * 코드를 라인별로 분리합니다.
        code_lines = code.split('\n')
        

        # * 지정된 라인 번호를 기준으로 코드를 추출합니다.
        extracted_lines = [
            line for line in code_lines 
            if ':' in line and start_line <= int(line.split(':')[0].split('~')[0].strip()) <= end_line
        ]
        
        return '\n'.join(extracted_lines)
    except Exception:
        logging.exception("Error occurred while extracting code within range(converting)")
        raise


# 역할: 전달된 노드의 스토어드 프로시저 코드에서 자식이 있을 경우 자식 부분을 요약합니다.
# 매개변수: 
#   file_content - 스토어드 프로시저 파일 전체 내용
#   node - (시작 라인, 끝 라인, 코드, 자식)을 포함한 노드 정보
# 반환값: 자식 코드들이 요약 처리된 노드 코드
def extract_and_summarize_code(file_content, node):

    def summarize_code(start_line, end_line, children):

        # * 시작 라인과 끝 라인을 기준으로 스토어드 프로시저 코드 라인을 추출합니다.
        code_lines = file_content[start_line-1:end_line]
        summarized_code = []
        last_end_line = start_line - 1


        # * 각 자식 노드에 대해 코드를 추출한 뒤, 요약 처리를 반복합니다.
        for child in children:
            before_child_code = code_lines[last_end_line-start_line+1:child['startLine']-start_line]
            summarized_code.extend([f"{i+last_end_line+1}: {line}" for i, line in enumerate(before_child_code)])
            summarized_code.append(f"{child['startLine']}: ... code ...\n")
            last_end_line = child['endLine']
        

        # * 마지막 자식 노드의 끝나는 지점 이후부터 노드의 끝나는 지점까지의 코드를 추가합니다
        after_last_child_code = code_lines[last_end_line-start_line+1:]
        summarized_code.extend([f"{i+last_end_line+1}: {line}" for i, line in enumerate(after_last_child_code)])
        return ''.join(summarized_code)
    
    try:

        # * 자식 노드가 없는 경우, 해당 노드의 코드만을 추출합니다.
        if not node.get('children'):
            code_lines = file_content[node['startLine']-1:node['endLine']]
            return ''.join([f"{i+node['startLine']}: {line}" for i, line in enumerate(code_lines)])
        else:
            # * 자식 노드가 있는 경우, summarize_code 함수를 호출하여 요약 처리합니다.
            return summarize_code(node['startLine'], node['endLine'], node.get('children', []))
    except Exception:
        logging.exception("during summarize code unexpected error occurred(converting)")
        raise


# 역할: 현재 스케줄에서 시작하여 스택에 있는 모든 스케줄을 역순으로 검토하면서 필요한 스토어드 프로시저 코드를 조합합니다.
# 매개변수: 
#      - current_schedule (dict): 현재 처리 중인 스케줄 정보
#      - schedule_stack (list): 처리된 스케줄들의 스택
# 반환값: 분석에 사용될 스토어드 프로시저 코드
def create_focused_code(current_schedule, schedule_stack):
    try:
        focused_code = current_schedule["code"]
        current_start_line = current_schedule["startLine"]


        # * 스택을 역순으로 검토하면서 스토어드 프로시저 코드를 조합합니다.
        for schedule in reversed(schedule_stack):
            placeholder = f"{current_start_line}: ... code ..."
            if placeholder in schedule["code"]:

                # * 현재 스케줄의 시작 라인을 플레이스홀더로 사용하여 실제 스토어드 프로시저 코드로 교체합니다.
                focused_code = schedule["code"].replace(placeholder, focused_code, 1)
                current_start_line = schedule["startLine"]

        return focused_code

    except Exception:
        logging.exception("An error occurred while creating focused code(converting)")
        raise


# 역할: 전달된 스토어드 프로시저 코드에서 불필요한 정보를 제거합니다.
# 매개변수: 
#      - code : 스토어드 프로시저 코드
# 반환값: 불필요한 정보가 제거된 스토어드 프로시저 코드.
def remove_code_placeholders(code):
    try:
        if code == "": return code 

        # * 모든 주석을 제거합니다.
        code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        code = re.sub(r'--.*$', '', code, flags=re.MULTILINE)
        # code = re.sub(r'^[\d\s:]*$', '', code, flags=re.MULTILINE)
        return code
    
    except Exception:
        logging.exception("Error during code placeholder removal(converting)")
        raise


# 역할: 프로시저 노드 코드에서 필요한 코드 부분만 추출하는 메서드입니다.
# 매개변수: 
#      - procedure_code : 프로시저 노드 부분의 스토어드 프로시저 코드.
# 반환값: 프로시저 노드 코드에서 변수 선언 부분만 필터링된 코드.
def process_procedure_node(procedure_code):
    try:
        # * ... code ... 가 처음 식별되는 라인을 찾은 뒤, 해당 라인의 이전 라인부터 삭제   
        index = procedure_code.find('... code ...')
        if index != -1:
            newline_before_index = procedure_code.rfind('\n', 0, index)
            newline_before_that = procedure_code.rfind('\n', 0, newline_before_index)
            procedure_code = procedure_code[:newline_before_that]


        # * 모든 주석을 제거합니다.
        procedure_code = re.sub(r'/\*.*?\*/', '', procedure_code, flags=re.DOTALL)
        procedure_code = re.sub(r'--.*$', '', procedure_code, flags=re.MULTILINE)
        return procedure_code

    except Exception:
        logging.exception("Error during code placeholder removal(converting)")
        raise


# 역할: 해당 노드에서 사용된 변수노드를 가져오는 메서드입니다.
# 매개변수: 
#      - node_id : 노드의 고유 식별자.
# 반환값: 노드에서 사용된 변수 노드의 목록.
async def fetch_variable_nodes(node_id):
    
    try:
        # * 변수 노드를 가져오는 사이퍼쿼리를 준비한 뒤, 퀴리 실행하여, 가져온 정보를 추출합니다
        query = [f"MATCH (v:Variable) WHERE v.role_{node_id} IS NOT NULL RETURN v"]
        connection = Neo4jConnection()  
        variable_nodes = await connection.execute_queries(query) 
        variable_node_list = [node['v']['name'] for node in variable_nodes[0]]
        logging.info("\nSuccess received Variable Nodes from Neo4J\n")
        await connection.close()
        return variable_node_list

    except Exception:
        logging.exception("Error during bring variable node from neo4j(converting)")
        raise


# 역할: Service 클래스를 생성하기 위해 분석을 시작하는 함수입니다.
# 매개변수: 
#   data - 분석할 데이터 구조(ANTLR)
#   file_content - 분석할 스토어드 프로시저 파일의 내용.
#   service_skeleton - 서비스 클래스의 기본 구조.
#   jpa_method_list - 사용된 JPA 메서드 목록.
#   procedure_variable - 프로시저 선언부에서 선언된 변수 정보(입력 매개변수).
# 반환값 : 없음
async def analysis(data, file_content, service_skeleton, jpa_method_list, procedure_variable):
    schedule_stack = []               # 스케줄 스택
    context_range = []                # LLM이 분석할 코드의 범위
    variable_list = {}                # 특정 노드에서 사용된 변수 목록
    jpa_query_methods = []            # 특정 노드에서 사용된 JPA 쿼리 메서드 목록
    procedure_variables = []          # 프로시저 선언부에서 선언된 변수 목록 
    extract_code = None               # 범위 만큼 추출된 코드
    clean_code= None                  # 불필요한 정보(주석)가 제거된 코드
    focused_code = None               # 전체적인 스토어드 프로시저 코드의 틀
    service_code = None               # 서비스 클래스 코드
    token_count = 0                   # 토큰 수
    LLM_count = 0                     # LLM 호출 횟수

    logging.info("\n Start creating a service class \n")


    # 역할: llm에게 분석할 스토어드 프로시저 코드를 전달한 뒤, 해당 결과를 바탕으로 Service를 생성합니다.
    # 반환값 : Service 클래스 파일
    async def process_analysis_results():
        nonlocal clean_code, token_count, LLM_count, focused_code, extract_code, service_code
        
        try:
            # * 정리된 코드를 분석합니다(llm에게 전달)
            analysis_result = convert_code(clean_code, service_code, variable_list, jpa_query_methods, procedure_variables)
            LLM_count += 1


            # * 분석 결과 각각의 데이터를 추출하고, 필요한 변수를 초기화합니다 
            service_code = analysis_result['code']


            # * 서비스 클래스를 파일로 생성합니다.
            service_directory = os.path.join('test', 'test_converting', 'converting_result', 'service')  
            os.makedirs(service_directory, exist_ok=True) 
            service_file_path = os.path.join(service_directory, f"{analysis_result['name']}.txt")  
            async with aiofiles.open(service_file_path, 'w', encoding='utf-8') as file:  
                await file.write(analysis_result['code'])  
                logging.info(f"\nSuccess Create {analysis_result['name']} Java File\n")  


            # * 다음 분석 주기를 위해 필요한 변수를 초기화합니다
            clean_code = None
            extract_code = None
            token_count = 0
            context_range.clear()
            variable_list.clear()

        except Exception:
            logging.exception("An error occurred during analysis results processing(converting)")
            raise
    

    # 역할: 토큰 수가 최대치를 초과할 경우, service 생성을 위해 처리하는 메서드를 호출합니다
    async def signal_for_process_analysis():
        try:
            analysis_task = asyncio.create_task(process_analysis_results())
            await asyncio.gather(analysis_task)
            
        except Exception:
            logging.exception(f"An error occurred during signal_for_process_analysis(converting)")
            raise


    # 역할: 재귀적으로 노드를 순회하며 구조를 탐색하고, 필요한 데이터를 처리하여 서비스 클래스를 생성하는 함수입니다.
    # 매개변수: 
    #   node - 분석할 노드.
    #   schedule_stack - 스케줄들의 스택.
    #   service_skeleton - 서비스 클래스의 기본 구조.
    #   parent_id - 현재 노드의 부모 노드 ID.
    #   jpa_method_list - 사용된 JPA 메서드 목록.
    #   procedure_variable - 프로시저 선언부에서 사용된 변수 정보.
    # 반환값: 없음
    async def traverse(node, schedule_stack, service_skeleton, parent_id, jpa_method_list, procedure_variable):
        nonlocal focused_code, token_count, clean_code, extract_code, service_code, procedure_variables
        
        # * 순회를 시작하기에 앞서 필요한 정보를 초기화하고 준비합니다.
        summarized_code = extract_and_summarize_code(file_content, node)
        check_node_size = count_tokens_in_text(remove_code_placeholders(summarized_code))
        children = node.get('children', [])
        current_schedule = {
            "startLine": node['startLine'],
            "endLine": node['endLine'],
            "code": summarized_code,
            "child": children,
        }

        # * 서비스 클래스 코드 할당 및 Command 클래스에서 선언된 변수 목록을 할당합니다
        if service_code is None:
            service_code = service_skeleton
            procedure_variables = list(procedure_variable)


        # * 현재 노드에서 쓰인 jpa 쿼리 메서드를 찾아서 추가합니다
        node_range = f"{node['startLine']}~{node['endLine']}"
        found = False  
        for jpa_method_dict in jpa_method_list:
            if found:  
                break
            for key, value in jpa_method_dict.items():
                formatted_key = key.split('_')[-1]
                if node_range == formatted_key:
                    jpa_query_methods.append({key: value})
                    found = True  
                    break


        # * 해당 노드에서 사용된 변수 노드 목록을 가져옵니다
        if node['type'] == "STATEMENT":
            variable_nodes = await fetch_variable_nodes(node['startLine'])
            variable_list[f"startLine_{node['startLine']}"] = variable_nodes


        # * focused_code에서 분석할 범위를 기준으로 startLine이 가장 작고, endLine이 가장 큰걸 기준으로 추출합니다
        extract_code = extract_code_within_range(focused_code, context_range)
        clean_code = remove_code_placeholders(extract_code)


        # * 노드 크기 및 토큰 수 체크를 하여, 분석 여부를 결정합니다
        token_count = count_tokens_in_text(clean_code)
        if (check_node_size >=300 and context_range) or (token_count >= 500 and context_range) or (len(context_range) > 8):
            signal_task = asyncio.create_task(signal_for_process_analysis())
            await asyncio.gather(signal_task)


        # * focused_code가 없으면 새로 생성하고, 만약 있다면 확장합니다
        if focused_code is None:
            focused_code = create_focused_code(current_schedule, schedule_stack) 
        else:
            placeholder = f"{node['startLine']}: ... code ..."
            focused_code = focused_code.replace(placeholder, summarized_code, 1)


        # * 자식이 없는 경우, 해당 노드의 범위를 분석할 범위로 저장합니다
        if not children and node['type'] == "STATEMENT":
            context_range.append({"startLine": node['startLine'], "endLine": node['endLine']})


        # * 스케줄 스택에 현재 스케줄을 넣습니다
        schedule_stack.append(current_schedule)


        # * 현재 노드가 자식이 있는 경우, 해당 자식을 순회하면서 traverse함수를 (재귀적으로) 호출하고 처리합니다
        for child in children:
            node_explore_task = asyncio.create_task(traverse(child, schedule_stack, service_skeleton, node['startLine'], jpa_method_list, procedure_variable))
            await asyncio.gather(node_explore_task)
        

        # * 조건하에 필요없는 스케줄 스택을 제거합니다 
        schedule_stack[:] = filter(lambda schedule: schedule['child'] and schedule['endLine'] > current_schedule['startLine'], schedule_stack)
        

        # * 부모 노드가 가진 자식들이 모두 처리가 끝났다면, 부모 노드도 context_range에 포함합니다
        if children and node['type'] == "STATEMENT":
            context_range.append({"startLine": node['startLine'], "endLine": node['endLine']})      

    try:
        # * traverse 함수를 호출하여, 노드 순회를 시작합니다
        start_analysis_task = asyncio.create_task(traverse(data, schedule_stack, service_skeleton , None, jpa_method_list, procedure_variable))
        await asyncio.gather(start_analysis_task)


        # * 마지막 노드그룹에 대한 처리를 합니다
        if context_range and focused_code is not None:
            extract_code = extract_code_within_range(focused_code, context_range)
            clean_code = remove_code_placeholders(extract_code)
            signal_task = asyncio.create_task(signal_for_process_analysis())
            await asyncio.gather(signal_task)
        logging.info("\nLLM 호출 횟수 : " + str(LLM_count))

    except Exception:
        logging.exception("An error occurred during the analysis process(converting)")
        raise


# 역할: 스토어드 프로시저 파일과 ANTLR 분석 파일을 읽어서 분석을 시작하는 메서드입니다.
# 매개변수: 
#   fileName - 스토어드 프로시저 파일 이름
# 반환값: 없음 
async def start_service_processing(sp_fileName):
    
    service_skeleton = """
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PayrollService {

    @PostMapping(path="/calculatePayroll")
    public void calculatePayroll(@RequestBody CalculatePayrollCommand command) {
        double overtimeHours = 0;
        double overtimeRate = 1.5;
        double overtimePay = 0;
        double unpaidLeaveDays = 0;
        double unpaidDeduction = 0;
        double taxRate = 0.1;
        double contractTaxRate = 0;
        double taxDeduction = 0;
        double baseSalary = 0;
        String employeeType = "";

        // Method logic goes here

        return;
    }
}
    """

  
    jpa_method_list = [
            {"19~22": "List<Employee> findByEmployeeId(@Param(\"p_employee_id\") Long pEmployeeId);"},
            {"26~30": "List<WorkLog> findByEmployeeIdAndWorkDateBetween(@Param(\"p_employee_id\") Long pEmployeeId)"},
            {"37~42": "List<LeaveRecords> findUnpaidLeaveDays(@Param(\"p_employee_id\") Long pEmployeeId)"}
    ]

    procedure_variable = {
            "procedureParameters": [
                "p_employee_id NUMBER",
                "p_include_overtime BOOLEAN DEFAULT TRUE",
                "p_include_unpaid_leave BOOLEAN DEFAULT TRUE",
                "p_include_tax BOOLEAN DEFAULT TRUE"
            ]
    }


    base_dir = os.path.dirname(__file__)  
    analysis_file_path = os.path.abspath(os.path.join(base_dir, '..', '..', '..', 'cypher', 'analysis', '.json'))
    sql_file_path = os.path.abspath(os.path.join(base_dir, '..', '..', '..', 'cypher', 'sql', '.txt'))
    

    # * 분석에 필요한 파일들(스토어드 프로시저, ANTLR 분석)의 내용을 읽습니다
    async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as analysis_file, aiofiles.open(sql_file_path, 'r', encoding='utf-8') as sql_file:
        analysis_data, sql_content = await asyncio.gather(analysis_file.read(), sql_file.readlines())
        analysis_data = json.loads(analysis_data)


    # * 읽어들인 데이터를 바탕으로 분석 메서드를 호출합니다.
    await analysis(analysis_data, sql_content, service_skeleton, jpa_method_list, procedure_variable)


# 서비스 생성을 위한 테스트 모듈입니다.
class TestAnalysisMethod(unittest.IsolatedAsyncioTestCase):
    async def test_create_service(self):
        await start_service_processing("P_B_CAC120_CALC_SUIP_STD")


if __name__ == "__main__":
    unittest.main()