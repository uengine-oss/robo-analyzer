import json
import logging
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.rule_loader import RuleLoader


class DbmsSkeletonGenerator:
    """
    DBMS 스켈레톤 생성기
    - PROCEDURE 및 DECLARE 컨텍스트를 기반으로 Oracle용 스켈레톤을 생성
    - DECLARE 변수 정보를 LLM에 전달하여 일관된 헤더/본문 구조 구성
    """

    __slots__ = (
        'folder_name',
        'file_name',
        'procedure_name',
        'project_name',
        'user_id',
        'api_key',
        'locale',
        'target_dbms',
        'rule_loader',
    )

    def __init__(
        self,
        folder_name: str,
        file_name: str,
        procedure_name: str,
        project_name: str,
        user_id: str,
        api_key: str,
        locale: str,
        target_dbms: str = "oracle",
    ):
        self.folder_name = folder_name
        self.file_name = file_name
        self.procedure_name = procedure_name
        self.project_name = project_name or "demo"
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.target_dbms = target_dbms
        self.rule_loader = RuleLoader(target_lang=target_dbms)

    async def generate(self) -> str:
        """Oracle용 DBMS 스켈레톤 생성"""
        connection = Neo4jConnection()

        try:
            context = await self._fetch_procedure_context(connection)

            inputs = {
                'procedure_name': self.procedure_name,
                'project_name': self.project_name,
                'spec_code': context.get('spec_code', ''),
                'declare_nodes': json.dumps(context.get('declare_nodes', []), ensure_ascii=False, indent=2),
                'declare_variables': json.dumps(context.get('declare_variables', []), ensure_ascii=False, indent=2),
                'locale': self.locale,
            }

            result = self.rule_loader.execute(
                role_name='dbms_skeleton',
                inputs=inputs,
                api_key=self.api_key,
            )

            skeleton_code = (result.get('code') or '').strip()
            if not skeleton_code:
                raise ConvertingError(f"DBMS 스켈레톤 생성 실패: 결과가 비어있습니다 ({self.procedure_name})")

            logging.info(f"🧱 DBMS 스켈레톤 생성 완료: {self.procedure_name}")
            return skeleton_code

        except ConvertingError:
            raise
        except Exception as e:
            err_msg = f"DBMS 스켈레톤 생성 중 오류: {str(e)}"
            logging.error(err_msg)
            raise ConvertingError(err_msg)
        finally:
            await connection.close()

    async def _fetch_procedure_context(self, connection: Neo4jConnection) -> dict:
        """PROCEDURE, SPEC, DECLARE 컨텍스트 수집"""
        procedure_query = f"""
            MATCH (p:PROCEDURE {{
              folder_name: '{self.folder_name}',
              file_name: '{self.file_name}',
              procedure_name: '{self.procedure_name}',
              user_id: '{self.user_id}'
            }})
            OPTIONAL MATCH (p)-[:PARENT_OF]->(spec:SPEC {{
              folder_name: '{self.folder_name}',
              file_name: '{self.file_name}',
              procedure_name: '{self.procedure_name}',
              user_id: '{self.user_id}'
            }})
            RETURN p, spec
        """

        declare_query = f"""
            MATCH (p:PROCEDURE {{
              folder_name: '{self.folder_name}',
              file_name: '{self.file_name}',
              procedure_name: '{self.procedure_name}',
              user_id: '{self.user_id}'
            }})-[:PARENT_OF]->(decl:DECLARE {{
              folder_name: '{self.folder_name}',
              file_name: '{self.file_name}',
              procedure_name: '{self.procedure_name}',
              user_id: '{self.user_id}'
            }})
            OPTIONAL MATCH (decl)-[:SCOPE]->(v:Variable {{
              folder_name: '{self.folder_name}',
              file_name: '{self.file_name}',
              procedure_name: '{self.procedure_name}',
              user_id: '{self.user_id}'
            }})
            WITH decl, v
            ORDER BY coalesce(toInteger(decl.startLine), 0), coalesce(toInteger(v.startLine), 0)
            RETURN decl, collect(v) AS variables
        """

        results = await connection.execute_queries([procedure_query, declare_query])
        procedure_rows = results[0] if results else []
        declare_rows = results[1] if len(results) > 1 else []

        if not procedure_rows:
            raise ConvertingError(f"프로시저 정보가 존재하지 않습니다: {self.procedure_name}")

        spec_code = ""
        if procedure_rows:
            first_row = procedure_rows[0]
            spec_node = first_row.get('spec')
            if spec_node:
                spec_code = spec_node.get('node_code', '')

        declare_nodes = []
        declare_variables = []
        seen_decl_ranges = set()
        seen_variables = set()

        for row in declare_rows:
            decl_node = row.get('decl')
            if decl_node:
                key = (
                    decl_node.get('startLine'),
                    decl_node.get('endLine'),
                    decl_node.get('node_code'),
                )
                if key not in seen_decl_ranges:
                    declare_nodes.append({
                        'startLine': decl_node.get('startLine'),
                        'endLine': decl_node.get('endLine'),
                        'node_code': decl_node.get('node_code', ''),
                        'token': decl_node.get('token'),
                    })
                    seen_decl_ranges.add(key)

            for variable in row.get('variables', []):
                if not variable:
                    continue
                var_key = (variable.get('name'), variable.get('type'), variable.get('parameter_type'))
                if var_key in seen_variables:
                    continue
                declare_variables.append({
                    'name': variable.get('name'),
                    'type': variable.get('type'),
                    'parameter_type': variable.get('parameter_type'),
                    'value': variable.get('value'),
                })
                seen_variables.add(var_key)

        return {
            'spec_code': spec_code,
            'declare_nodes': declare_nodes,
            'declare_variables': declare_variables,
        }


async def start_dbms_skeleton(
    folder_name: str,
    file_name: str,
    procedure_name: str,
    project_name: str,
    user_id: str,
    api_key: str,
    locale: str,
    target_dbms: str = "oracle",
) -> str:
    """DBMS 스켈레톤 생성 진입점"""
    generator = DbmsSkeletonGenerator(
        folder_name=folder_name,
        file_name=file_name,
        procedure_name=procedure_name,
        project_name=project_name,
        user_id=user_id,
        api_key=api_key,
        locale=locale,
        target_dbms=target_dbms,
    )
    return await generator.generate()

