import os, sys, asyncio
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from _common import setup_logging, get_context, persist_context

from convert.create_repository import start_repository_processing


async def main() -> None:
    setup_logging()

    # 입력 준비
    ctx = get_context()
    file_names = ctx['file_names']
    user_id = ctx['user_id']
    api_key = ctx['api_key']
    project_name = ctx['project_name']
    locale = ctx['locale']

    # 실행
    used_query_methods, global_variables, sequence_methods, repository_list = await start_repository_processing(
        file_names, user_id, api_key, project_name, locale
    )

    # 저장 (리포 및 변수/시퀀스)
    persist_context(ctx, {
        'used_query_methods': used_query_methods,
        'global_variables': global_variables,
        'sequence_methods': sequence_methods,
    })

    print(f"repository ok: repos={len(repository_list)} seq={len(sequence_methods)}")


if __name__ == '__main__':
    asyncio.run(main())


