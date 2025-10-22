import os
import logging
import textwrap
import json
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import convert_to_camel_case, convert_to_pascal_case, save_file, build_java_base_path
from util.prompt_loader import PromptLoader


# ----- Service Skeleton 생성 관리 클래스 -----
class ServiceSkeletonGenerator:
    """
    레거시 프로시저/함수를 분석하여 Spring Boot Service 스켈레톤을 자동 생성하는 클래스
    Neo4j에서 프로시저/함수 노드와 변수 정보를 조회하고,
    LLM을 활용하여 Service 클래스의 기본 구조와 메서드를 생성합니다.
    """
    __slots__ = ('project_name', 'user_id', 'api_key', 'locale',
                 'folder_name', 'file_name', 'dir_name', 'service_class_name',
                 'external_packages', 'exist_command_class', 'global_vars', 'prompt_loader')

    def __init__(self, project_name: str, user_id: str, api_key: str, locale: str = 'ko', target_lang: str = 'java'):
        """
        ServiceSkeletonGenerator 초기화
        
        Args:
            project_name: 프로젝트 이름
            user_id: 사용자 식별자
            api_key: LLM API 키
            locale: 언어 설정 (기본값: 'ko')
            target_lang: 타겟 언어 (기본값: 'java')
        """
        self.project_name = project_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.prompt_loader = PromptLoader(target_lang=target_lang)

    # ----- 공개 메서드 -----

    async def generate(self, entity_name_list: list, folder_name: str, file_name: str, 
                      global_variables: list) -> tuple:
        """
        Service Skeleton 생성의 메인 진입점
        Neo4j에서 프로시저/함수 정보를 조회하고, LLM 변환을 수행하여
        Service 클래스 스켈레톤과 Command 클래스 파일을 생성합니다.
        
        Args:
            entity_name_list: 서비스에서 사용할 엔티티 클래스명 목록
            folder_name: 폴더(시스템)명
            file_name: 파일명
            global_variables: 전역 변수 목록
        
        Returns:
            tuple: (method_info_list, service_class_name, exist_command_class, command_class_list)
        
        Raises:
            ConvertingError: Service Skeleton 생성 중 오류 발생 시
        """
        logging.info("\n" + "="*80)
        logging.info("🏗️  STEP 3: Service Skeleton 생성 시작")
        logging.info("="*80)
        connection = Neo4jConnection()
        
        # 속성 초기화
        self.folder_name = folder_name
        self.file_name = file_name
        object_name = os.path.splitext(file_name)[0]
        self.dir_name = convert_to_camel_case(object_name)
        self.service_class_name = convert_to_pascal_case(object_name) + "Service"

        try:
            # 프로시저 및 외부 호출 조회
            procedure_groups, self.external_packages = await self._fetch_procedures(connection)
            self.exist_command_class = any(g['parameters'] for g in procedure_groups.values())
            
            # 전역 변수 변환 (Role 파일 사용)
            if global_variables:
                self.global_vars = self.prompt_loader.execute(
                    role_name='variable',
                    inputs={
                        'variables': json.dumps(global_variables, ensure_ascii=False, indent=2),
                        'locale': self.locale
                    },
                    api_key=self.api_key
                )
            else:
                self.global_vars = {"variables": []}
            
            # 서비스 템플릿 생성
            service_skeleton = self._build_template(entity_name_list)

            # 프로시저별 메서드/커맨드 생성
            method_info_list = []
            command_class_list = []
            
            for proc_name, proc_data in procedure_groups.items():
                method_info = await self._process_procedure(proc_name, proc_data, service_skeleton)
                method_info_list.append(method_info)
                
                # Command 클래스 추가
                if (cmd_name := method_info.get('command_class_name')) and (cmd_code := method_info.get('command_class_code')):
                    command_class_list.append({'commandName': cmd_name, 'commandCode': cmd_code})
            
            logging.info(f"Service Skeleton 생성이 완료되었습니다: {self.service_class_name}\n")
            logging.info("\n" + "-"*80)
            logging.info(f"✅ STEP 3 완료: {self.service_class_name} Skeleton 생성 완료")
            logging.info(f"   - 프로시저/함수: {len(method_info_list)}개")
            logging.info(f"   - Command 클래스: {len(command_class_list)}개")
            logging.info("-"*80 + "\n")
            
            return method_info_list, self.service_class_name, self.exist_command_class, command_class_list
        
        except ConvertingError:
            raise
        except Exception as e:
            logging.error(f"[{object_name}] Service Skeleton 생성 중 오류: {str(e)}")
            raise ConvertingError(f"[{object_name}] Service Skeleton 생성 중 오류: {str(e)}")
        finally:
            await connection.close()

    # ----- 내부 처리 메서드 -----

    async def _fetch_procedures(self, connection: Neo4jConnection) -> tuple:
        """
        프로시저/함수 노드 및 외부 호출 정보 조회
        
        Args:
            connection: Neo4j 연결 객체
        
        Returns:
            tuple: (procedure_groups, external_packages)
        """
        procedure_nodes, external_nodes = await connection.execute_queries([
            f"""MATCH (p {{folder_name: '{self.folder_name}', file_name: '{self.file_name}'}})
                WHERE p:PROCEDURE OR p:CREATE_PROCEDURE_BODY OR p:FUNCTION
                OPTIONAL MATCH (p)-[:PARENT_OF]->(d:DECLARE {{folder_name: '{self.folder_name}', file_name: '{self.file_name}'}})
                OPTIONAL MATCH (d)-[:SCOPE]-(dv:Variable {{folder_name: '{self.folder_name}', file_name: '{self.file_name}'}})
                OPTIONAL MATCH (p)-[:PARENT_OF]->(s:SPEC {{folder_name: '{self.folder_name}', file_name: '{self.file_name}'}})
                OPTIONAL MATCH (s)-[:SCOPE]-(sv:Variable {{folder_name: '{self.folder_name}', file_name: '{self.file_name}'}})
                WITH p, d, dv, s, sv, 
                    CASE WHEN p:FUNCTION THEN 'FUNCTION' WHEN p:PROCEDURE THEN 'PROCEDURE' ELSE 'CREATE_PROCEDURE_BODY' END as node_type
                RETURN p, d, dv, s, sv, node_type ORDER BY p.startLine""",
            f"""MATCH (p {{folder_name: '{self.folder_name}', file_name: '{self.file_name}'}})-[:CALL {{scope: 'external'}}]->(ext)
                WITH ext.object_name as obj_name, COLLECT(ext)[0] as ext
                RETURN ext"""
        ])
        
        # 프로시저 그룹 구성
        groups = {}
        for item in procedure_nodes:
            proc_name = item['p'].get('procedure_name', '')
            
            if proc_name not in groups:
                groups[proc_name] = {
                    'parameters': [],
                    'local_variables': [],
                    'param_keys': set(),
                    'var_keys': set(),
                    'declaration': (item.get('s') or {}).get('node_code', ''),
                    'node_type': item['node_type']
                }
            
            group = groups[proc_name]
            
            # 파라미터 추가
            if sv := item.get('sv'):
                sv_type, sv_name = sv['type'], sv['name']
                sv_param_type = sv.get('parameter_type', '')
                key = (sv_type, sv_name)
                if key not in group['param_keys']:
                    group['param_keys'].add(key)
                    group['parameters'].append({'type': sv_type, 'name': sv_name, 'parameter_type': sv_param_type})
                    
                    # OUT 파라미터는 지역변수로도 추가 (Java는 OUT 파라미터가 없으므로)
                    if sv_param_type == 'OUT' and key not in group['var_keys']:
                        group['var_keys'].add(key)
                        sv_value = sv.get('value', '')
                        group['local_variables'].append({'type': sv_type, 'name': sv_name, 'value': sv_value})
            
            # 로컬 변수 추가 (DECLARE 노드에서)
            if dv := item.get('dv'):
                dv_type, dv_name, dv_value = dv['type'], dv['name'], dv['value']
                key = (dv_type, dv_name)
                if key not in group['var_keys']:
                    group['var_keys'].add(key)
                    group['local_variables'].append({'type': dv_type, 'name': dv_name, 'value': dv_value})
        
        # 임시 set 제거
        for g in groups.values():
            g.pop('param_keys', None)
            g.pop('var_keys', None)
        
        # 외부 패키지 추출
        external_packages = [ext['object_name'] for n in external_nodes if (ext := n.get('ext')) and ext.get('object_name')]
        
        return groups, external_packages

    def _build_template(self, entity_list: list) -> str:
        """
        Service 클래스 템플릿 생성
        
        Args:
            entity_list: 엔티티 목록
        
        Returns:
            str: Service 클래스 템플릿 코드
        """
        imports = []
        fields = []
        
        # Global variable fields
        if self.global_vars and (variables := self.global_vars.get("variables")):
            fields.extend(f"    private {v['javaType']} {v['javaName']} = {v['value']};" for v in variables)
        
        # Entity imports/fields
        if entity_list:
            project_prefix = f"com.example.{self.project_name}"
            for e in entity_list:
                entity_name = e['entityName']
                repo_name = f"{entity_name}Repository"
                imports.append(f"import {project_prefix}.entity.{entity_name};")
                imports.append(f"import {project_prefix}.repository.{repo_name};")
                fields.append(f"    @Autowired\n    private {repo_name} {entity_name[0].lower()}{entity_name[1:]}Repository;")
        
        # Command import
        if self.exist_command_class:
            imports.append(f"import com.example.{self.project_name}.command.{self.dir_name}.*;")
        
        # External service fields
        if self.external_packages:
            for p in self.external_packages:
                pascal, camel = convert_to_pascal_case(p), convert_to_camel_case(p)
                fields.append(f"    @Autowired\n    private {pascal}Service {camel}Service;")
        
        return f"""package com.example.{self.project_name}.service;

{chr(10).join(imports)}
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.persistence.EntityNotFoundException;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.BeanUtils;
import java.time.format.DateTimeFormatter;
import org.springframework.stereotype.Service;
import java.time.temporal.TemporalAdjusters;
import java.time.*;
import java.util.*;
import java.util.Map;
import java.util.HashMap;

@Transactional
@Service
public class {self.service_class_name} {{
    {chr(10).join(fields)}

CodePlaceHolder
}}"""

    async def _process_procedure(self, proc_name: str, proc_data: dict, service_skeleton: str) -> dict:
        """
        프로시저별 메서드 및 Command 클래스 생성
        
        Args:
            proc_name: 프로시저명
            proc_data: 프로시저 정보
            service_skeleton: Service 스켈레톤 코드
        
        Returns:
            dict: 메서드 및 Command 정보
        """
        node_type = proc_data['node_type']
        parameters = proc_data['parameters']
        
        # 🚀 성능 최적화: 파라미터를 한 번에 분류 (IN vs OUT)
        in_params = []
        out_params = []
        for p in parameters:
            param_type = p.get('parameter_type', '')
            if param_type == 'OUT':
                out_params.append(p)
            else:  # IN, IN_OUT, 또는 parameter_type이 없는 경우
                in_params.append(p)
        
        out_count = len(out_params)
        
        # Command 클래스 생성 (IN 파라미터만 사용) - Role 파일 사용
        cmd_var = cmd_name = cmd_code = None
        if node_type != 'FUNCTION' and in_params:
            analysis_cmd = self.prompt_loader.execute(
                role_name='command',
                inputs={
                    'command_class_data': json.dumps({'parameters': in_params, 'procedure_name': proc_name}, ensure_ascii=False, indent=2),
                    'dir_name': self.dir_name,
                    'project_name': self.project_name,
                    'locale': self.locale
                },
                api_key=self.api_key
            )
            cmd_name, cmd_code, cmd_var = analysis_cmd['commandName'], analysis_cmd['command'], analysis_cmd['command_class_variable']
            
            # Command 파일 저장
            cmd_path = build_java_base_path(self.project_name, self.user_id, 'command', self.dir_name)
            await save_file(cmd_code, f"{cmd_name}.java", cmd_path)
        
        # Service 메서드 생성 (IN 파라미터, 지역변수, OUT 파라미터를 별도로 전달) - Role 파일 사용
        analysis_method = self.prompt_loader.execute(
            role_name='service_skeleton',
            inputs={
                'method_skeleton_data': json.dumps({'procedure_name': proc_name, 'local_variables': proc_data['local_variables'], 'declaration': proc_data['declaration']}, ensure_ascii=False, indent=2),
                'parameter_data': json.dumps({'in_parameters': in_params, 'out_parameters': out_params, 'out_count': out_count, 'procedure_name': proc_name}, ensure_ascii=False, indent=2),
                'locale': self.locale
            },
            api_key=self.api_key
        )
        
        method_text, method_name, method_signature = analysis_method['method'], analysis_method['methodName'], analysis_method['methodSignature']
        method_code = textwrap.indent(method_text, '    ')
        
        return {
            'command_class_variable': cmd_var,
            'command_class_name': cmd_name,
            'method_skeleton_name': method_name,
            'method_skeleton_code': method_code,
            'method_signature': method_signature,
            'service_method_skeleton': service_skeleton.replace("CodePlaceHolder", method_code),
            'node_type': node_type,
            'procedure_name': proc_name,
            'command_class_code': cmd_code
        }
