import os
import sys
import json
import logging


BACKEND_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if BACKEND_ROOT not in sys.path:
    sys.path.append(BACKEND_ROOT)


RESULT_FILE = os.path.join('test', 'test_converting', 'test_results.json')
CONFIG_FILE = os.path.join('test', 'test_converting', 'config.json')


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s', force=True)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    for name in [
        'asyncio','anthropic','langchain','urllib3','anthropic._base_client','anthropic._client',
        'langchain_core','langchain_anthropic','uvicorn','fastapi'
    ]:
        logging.getLogger(name).setLevel(logging.CRITICAL)


def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def load_results() -> dict:
    if os.path.exists(RESULT_FILE):
        with open(RESULT_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except Exception:
                return {}
    return {}


def save_results(data: dict) -> None:
    os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)
    with open(RESULT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_context() -> dict:
    """폴더/파일/세션/프로젝트/로케일/키를 일관되게 가져옵니다."""
    cfg = load_config()
    res = load_results()

    test_data = res.get('test_data') or cfg.get('test_data') or [["FN_DAYSUM_GENTIME", "FN_DAYSUM_GENTIME.sql"]]
    test_data = [(folder, file) for folder, file in test_data]

    user_id = res.get('user_id') or cfg.get('user_id') or os.getenv('TEST_SESSION_UUID', '34576d0c-a941-455d-a89b-42fd22d6674f')
    api_key = os.getenv('TEST_API_KEY', os.getenv('LLM_API_KEY', 'your-api-key'))
    project_name = res.get('project_name') or cfg.get('project_name') or os.getenv('TEST_PROJECT_NAME', 'demo')
    locale = res.get('locale') or cfg.get('locale') or os.getenv('TEST_LOCALE', 'ko')

    return {
        'test_data': test_data,
        'user_id': user_id,
        'api_key': api_key,
        'project_name': project_name,
        'locale': locale,
        'results': res,
        'config': cfg,
    }


def persist_context(ctx: dict, updates: dict | None = None) -> None:
    """컨텍스트 핵심 값과 단계별 업데이트를 결과 JSON에 병합 저장."""
    base = ctx.get('results', {}).copy()
    base.update({
        'test_data': [[folder, file] for folder, file in ctx['test_data']],
        'user_id': ctx['user_id'],
        'project_name': ctx['project_name'],
        'locale': ctx['locale'],
    })
    if updates:
        base.update(updates)
    save_results(base)


