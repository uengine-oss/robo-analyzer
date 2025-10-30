import logging
import json
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import calculate_code_token, save_file, build_rule_based_path
from util.rule_loader import RuleLoader


# ----- 상수 정의 -----
MAX_TOKENS = 1000  # LLM 처리를 위한 배치당 최대 토큰 수


# ----- Entity 생성 관리 클래스 -----
class EntityGenerator:
    """
    레거시 데이터베이스 테이블 정보를 기반으로 JPA Entity 클래스를 자동 생성하는 클래스
    Neo4j에서 테이블 스키마 정보를 조회하고, LLM을 활용하여 Spring Boot JPA Entity로 변환합니다.
    """
    __slots__ = ('project_name', 'user_id', 'api_key', 'locale', 'save_path', 'entity_results', 'rule_loader')

    def __init__(self, project_name: str, user_id: str, api_key: str, locale: str = 'ko', target_lang: str = 'java'):
        """
        EntityGenerator 초기화
        
        Args:
            project_name: 프로젝트 이름
            user_id: 사용자 식별자
            api_key: LLM API 키
            locale: 언어 설정 (기본값: 'ko')
            target_lang: 타겟 언어 (기본값: 'java')
        """
        self.project_name = project_name or ''
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.rule_loader = RuleLoader(target_lang=target_lang)
        self.save_path = build_rule_based_path(self.project_name, self.user_id, target_lang, 'entity')

    # ----- 공개 메서드 -----

    async def generate(self) -> list[dict]:
        """
        Entity 클래스 생성의 메인 진입점
        Neo4j에서 테이블 정보를 조회하고, 배치 단위로 LLM 변환을 수행하여
        Java Entity 클래스 파일을 생성합니다.
        
        Returns:
            list[dict]: 생성된 Entity 정보 리스트
                       [{'entityName': str, 'entityCode': str}, ...]
        
        Raises:
            ConvertingError: Entity 생성 중 오류 발생 시
        """
        logging.info("\n" + "="*80)
        logging.info("📦 STEP 1: Entity 클래스 생성 시작")
        logging.info("="*80)
        connection = Neo4jConnection()
        
        try:
            # Neo4j에서 테이블 및 컬럼 정보 조회 (프로젝트 전체)
            table_rows = (await connection.execute_queries([f"""
                MATCH (t:Table {{user_id: '{self.user_id}', project_name: '{self.project_name}'}})
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{self.user_id}', project_name: '{self.project_name}'}})
                WITH t, collect({{
                    name: c.name,
                    dtype: coalesce(c.dtype, ''),
                    nullable: toBoolean(c.nullable),
                    comment: coalesce(c.description, ''),
                    pk: coalesce(c.pk_constraint,'') <> ''
                }}) AS columns
                RETURN coalesce(t.schema,'') AS schema, t.name AS name, columns
                ORDER BY name
            """]))[0]
            
            if not table_rows:
                logging.info("⚠️  발견된 테이블 없음")
                return []
            
            logging.info(f"📊 조회된 테이블: {len(table_rows)}개")
            
            # 배치 단위로 처리하여 Entity 생성
            self.entity_results = []
            await self._process_tables(table_rows)
            
            logging.info("\n" + "-"*80)
            logging.info(f"✅ STEP 1 완료: {len(self.entity_results)}개 Entity 클래스 생성 완료")
            logging.info("-"*80 + "\n")
            return self.entity_results
        
        except ConvertingError:
            raise
        except Exception as e:
            logging.error(f"엔티티 클래스 생성 중 오류: {str(e)}")
            raise ConvertingError(f"엔티티 클래스 생성 중 오류: {str(e)}")
        finally:
            await connection.close()

    # ----- 내부 처리 메서드 -----

    async def _process_tables(self, table_rows: list) -> None:
        """
        테이블 목록을 배치 단위로 처리하여 Entity 생성
        토큰 수 제한을 고려하여 테이블을 배치로 묶고, 각 배치를 LLM으로 변환합니다.
        결과는 self.entity_results에 직접 누적됩니다.
        
        Args:
            table_rows: Neo4j에서 조회한 테이블 정보 리스트
        """
        current_tokens = 0
        batch = []

        for row in table_rows:
            # 테이블 정보 구성
            columns = row.get('columns') or []
            name = row.get('name')
            schema = row.get('schema') or ''
            
            # Primary Key 추출
            pk_list = [col['name'] for col in columns if col.get('pk')] if columns else []
            
            # 테이블 정보 딕셔너리
            table_info = {'name': name, 'schema': schema, 'fields': columns}
            if pk_list:
                table_info['primary_keys'] = pk_list
            
            tokens = calculate_code_token(table_info)
            
            # 배치 토큰 한도 초과 시 즉시 처리
            if batch and (current_tokens + tokens) >= MAX_TOKENS:
                await self._flush_batch(batch)
                batch, current_tokens = [], 0
            
            batch.append(table_info)
            current_tokens += tokens

        # 마지막 남은 배치 처리
        if batch:
            await self._flush_batch(batch)

    async def _flush_batch(self, batch: list) -> None:
        """
        배치를 LLM으로 변환하고 파일 저장 후 결과 누적
        배치 내 테이블들을 LLM에 전달하여 Entity 코드를 생성하고,
        생성된 코드를 Java 파일로 저장한 후 self.entity_results에 추가합니다.
        
        Args:
            batch: LLM 변환할 테이블 정보 리스트
        """
        # Role 파일 기반 프롬프트 실행
        analysis_data = self.rule_loader.execute(
            role_name='entity',
            inputs={
                'table_json_data': json.dumps(batch, ensure_ascii=False, indent=2),
                'project_name': self.project_name,
                'locale': self.locale
            },
            api_key=self.api_key
        )
        
        for entity in analysis_data['analysis']:
            name, code = entity['entityName'], entity['code']
            await save_file(code, f"{name}.java", self.save_path)
            self.entity_results.append({'entityName': name, 'entityCode': code})
