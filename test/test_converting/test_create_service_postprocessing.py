import logging
import os
import re
import sys
import textwrap
import unittest
import aiofiles
import tiktoken

logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from understand.neo4j_connection import Neo4jConnection


# * 인코더 설정 및 파일 이름 및 변수 초기화 
encoder = tiktoken.get_encoding("cl100k_base")
service_skeleton = None



# 역할: 크기가 매우 큰 노드의 요약 처리된 자바 코드에 실제 자식 자바 코드로 채워넣기 위한 함수
# 매개변수: 
#   - node_startLine : 부모 노드의 시작라인
#   - summarized_java_code: 자식 자바 코드들이 모두 요약처리된 부모 자바 코드
# 반환값 : 
#   - summarized_java_code : 실제 코드로 대체되어 완성된 부모 자바 코드
async def process_big_size_node(node_startLine, summarized_java_code, connection):
    
    # * 부모 노드의 시작 라인을 기준으로 자식 노드를 찾는 쿼리
    query = [f"""
    MATCH (n)-[r:PARENT_OF]->(m)
    WHERE n.startLine = {node_startLine}
    RETURN m
    """]
    
    # * 자식 노드 리스트를 비동기적으로 그래프 데이터베이스에서 가져옴
    child_node_list = await connection.execute_queries(query)
    

    # * 자식 노드 리스트를 순회하면서 각 노드 처리
    for node in child_node_list[0]:
        child = node['m']
        token = child['token']
        print("\n 크기가 매우 큰 노드 처리중")
        print(f"자식 노드 : [ 시작라인 : {child['startLine']}, 토큰 : {child['token']}, 끝라인 : {child['endLine']}")
        
        # * 자식 노드의 토큰 크기에 따라 재귀적으로 처리하거나 기존 코드 사용하여 요약된 부모 코드에서 실제 자식 코드로 교체
        java_code = await process_big_size_node(child['startLine'], child['java_code'], connection) if token > 1700 else child['java_code']
        placeholder = f"{child['startLine']}: ...code..."
        indented_code = textwrap.indent(java_code, '    ')
        summarized_java_code = summarized_java_code.replace(placeholder, f"\n{indented_code}")
    
    return summarized_java_code


# 역할: 서비스 클래스의 자바 코드를 생성하는 함수입니다.
# 매개변수: 
#   - node_list : 노드와 그들의 관계를 포함하는 리스트
# 반환값: 
#   - all_java_code : 생성된 전체 자바 코드 문자열
async def process_service_class(node_list, connection):

    previous_node_endLine = 0
    all_java_code = ""
    process_started = 0

    # * service class를 생성하기 위한 노드의 순회 시작
    for node in node_list:
        start_node = node['n']
        relationship = node['r'][1] if node['r'] else "NEXT"
        end_node = node['m']
        token = start_node['token']
        node_name = start_node['name']
        print("\n"+"-" * 40) 
        print(f"시작 노드 : [ 시작 라인 : {start_node['startLine']}, 이름 : ({start_node['name']}), 끝라인: {start_node['endLine']}, 토큰 : {start_node['token']}")
        print(f"관계: {relationship}")
        if end_node: print(f"종료 노드 : [ 시작 라인 : {end_node['startLine']}, 이름 : ({end_node['name']}), 끝라인: {end_node['endLine']}, 토큰 : {end_node['token']}")
        is_duplicate_or_unnecessary = (previous_node_endLine > start_node['startLine'] and previous_node_endLine) or ("EXECUTE_IMMDDIATE" in node_name and not process_started)


        # * 중복(이미 처리된 자식노드) 또는 불필요한 노드 건너뛰기
        if is_duplicate_or_unnecessary:
            print("자식노드 및 필요없는 노드로 넘어갑니다")
            continue

        
        # * 노드의 토큰 크기에 따라 처리 방식 결정
        if token > 1700:
            java_code = await process_big_size_node(start_node['startLine'], start_node['java_code'], connection)
        else:
            java_code = start_node['java_code']


        # * 처리 상태 초기화 및 Java 코드 추가
        process_started = 1
        all_java_code += java_code + "\n\n"
        previous_node_endLine = start_node['endLine']


    return all_java_code


# 역할: 노드 정보를 사용하여 서비스 클래스를 생성합니다. 
# 매개변수: 없음
async def start_create_service_class():
    

    # * 전역 변수
    global service_skeleton


    # * Neo4j 연결 생성
    connection = Neo4jConnection() 
    
    try:
        # * 노드와 관계를 가져오는 쿼리 
        node_query = [
            """
            MATCH (n)
            WHERE NOT (n:ROOT OR n:Variable OR n:DECLARE OR n:Table OR n:CREATE_PROCEDURE_BODY)
            OPTIONAL MATCH (n)-[r:NEXT]->(m)
            WHERE NOT (m:ROOT OR m:Variable OR m:DECLARE OR m:Table OR m:CREATE_PROCEDURE_BODY)
            RETURN n, r, m
            ORDER BY n.startLine
            """
        ]

        
        # * 쿼리 실행
        results = await connection.execute_queries(node_query)
        

        # * 결과를 함수로 전달
        all_java_code = await process_service_class(results[0], connection)
        

        # * 서비스 스켈레톤
        service_skeleton_code = f"""
package com.example.pbcac120calcsuipstd.service;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.example.pbcac120calcsuipstd.command.PBCac120CalcSuipStdCommand;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import org.springframework.transaction.annotation.Transactional;

@RestController
@Transactional
public class PBCac120CalcSuipStdService {{

    @PostMapping(path="/Endpoint")
    public void process(@RequestBody PBCac120CalcSuipStdCommand command) {{
        String vResultValue = "";
        long vDivnCnt = 0;
        long vCnt = 0;
        long vIdSeq = 0;
        String vTopDivnCd = "";
        String vSql = "";
        String vSqlInsert = "";
        String vSqlSelect = "";
        String vSqlFrom = "";
        String vSqlGroup = "";
        String vSqlSet = "";
        String vSqlWhere = "";
        String vSqlWhere01 = "";
        String vSqlWhere02 = "";
        String vSqlWhere03 = "";
        String vSqlWhere04 = "";
        String vSqlWhere05 = "";
        String vSqlWhere06 = "";
        String vSqlUpdate = "";
        String vSqlJoin = "";
        String vStdDataCol = "";
        String vStdSuipDataCol = "";
        String vStdInsertRst = "";
        String vStdSelectRst = "";
        String vStdGroupRst = "";
        String vCarStdDataCol = "";
        String vCarStdSuipDataCol = "";
        String vCarStdInsertRst = "";
        String vCarStdSelectRst = "";
        String vCarStdGroupRst = "";
        String vGenStdDataCol = "";
        String vGenStdSuipDataCol = "";
        String vGenStdInsertRst = "";
        String vGenStdSelectRst = "";
        String vGenStdGroupRst = "";
        long iIdx = 0;
        long iMax = 0;
        String vInsrCmpnCd = "";
        String vSuipCommGbn = "";
        String vStdComCd = "";
        String vDataCd = "";
        String vSuipDataCd = "";
        String vAggregateExp = "";
        String vDtType = "";
        String vDtStandard = "";
        long vSortNo = 0;
        long vCalcOrder = 0;
        String vCalcSign = "";
        String vCalcSignVal = "";
        String vSrcExcelCd = "";
        String vSrcSuipComCd = "";
        String vSrcFlt1Cd = "";
        String vSrcFlt1CompCd = "";
        String vSrcFlt1Val = "";
        String vSrcFlt2Cd = "";
        String vSrcFlt2CompCd = "";
        String vSrcFlt2Val = "";
        String vSrcFlt3Cd = "";
        String vSrcFlt3CompCd = "";
        String vSrcFlt3Val = "";
        String vSrcFlt4Cd = "";
        String vSrcFlt4CompCd = "";
        String vSrcFlt4Val = "";
        String vSrcFlt5Cd = "";
        String vSrcFlt5CompCd = "";
        String vSrcFlt5Val = "";
        String vSrcFlt6Cd = "";
        String vSrcFlt6CompCd = "";
        String vSrcFlt6Val = "";
        String vSrcSuipComCdMatch = "";
        String vSrcSuipComCdOrg = "";
        String vStndCarCategory = "";

{textwrap.indent(all_java_code, '        ')}
    }}
}}
        """
        service_skeleton = service_skeleton_code
        
        
        # * 서비스 클래스 파일을 생성합니다.  
        service_directory = os.path.join('test', 'test_converting', 'converting_result','service')
        os.makedirs(service_directory, exist_ok=True)
        service_path = os.path.join(service_directory, "PBCac120CalcSuipStdService.java")
        async with aiofiles.open(service_path, 'w', encoding='utf-8') as file:
            await file.write(service_skeleton)
            logging.info(f"\nSuccess Create PBCac120CalcSuipStdService Java File\n")


    except Exception as e:
        print(f"An error occurred from neo4j for service creation: {e}")
    finally:
        await connection.close() 


# 서비스 생성을 위한 테스트 모듈입니다.
class TestAnalysisMethod(unittest.IsolatedAsyncioTestCase):
    async def test_create_service(self):
        await start_create_service_class()


if __name__ == "__main__":
    unittest.main()