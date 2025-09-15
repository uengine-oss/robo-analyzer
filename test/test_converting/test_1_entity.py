import os, sys, asyncio
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from _common import setup_logging, get_context, persist_context
from convert.create_entity import start_entity_processing
from prompt.convert_project_name_prompt import generate_project_name_prompt


async def main() -> None:
    setup_logging()

    # 입력 준비 (중복 제거)
    ctx = get_context()
    file_names = ctx['file_names']
    user_id = ctx['user_id']
    api_key = ctx['api_key']
    project_name = ctx['project_name']
    locale = ctx['locale']

    if not project_name:
        project_name = await generate_project_name_prompt(file_names, api_key)

    # 실행
    entity_results = await start_entity_processing(file_names, user_id, api_key, project_name, locale)

    # 저장 (엔티티 목록)
    entity_name_list = {entity['entityName']: {"entityName": entity['entityName']} for entity in entity_results}
    persist_context(ctx, {
        'entity_name_list': entity_name_list,
    })

    print(f"entity ok: {len(entity_results)}")


if __name__ == '__main__':
    asyncio.run(main())


