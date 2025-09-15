import os, sys, asyncio
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from _common import setup_logging, get_context, persist_context
from convert.create_service_skeleton import start_service_skeleton_processing


async def main() -> None:
    setup_logging()

    ctx = get_context()
    file_names = ctx['file_names']
    user_id = ctx['user_id']
    api_key = ctx['api_key']
    project_name = ctx['project_name']
    locale = ctx['locale']

    # entity_name_list는 {entityName: {entityName}} 형태 → 리스트로 변환
    entity_name_map = ctx['results'].get('entity_name_list', {})
    entity_list = [{"entityName": name} for name in entity_name_map.keys()]

    # 객체별 스켈레톤 생성
    skeleton_results = {}
    for _, object_name in file_names:
        service_info, service_skeleton, service_class_name, exist_command_class, command_class_list = (
            await start_service_skeleton_processing(
                entity_list, object_name, ctx['results'].get('global_variables', []),
                user_id, api_key, project_name, locale
            )
        )

        skeleton_results[object_name] = {
            "service_info": service_info,
            "service_skeleton": service_skeleton,
            "service_class_name": service_class_name,
            "exist_command_class": exist_command_class,
            "command_class_list": command_class_list,
        }

    # 저장
    persist_context(ctx, {'service_skeleton_results': skeleton_results})

    print(f"skeleton ok: {len(skeleton_results)}")


if __name__ == '__main__':
    asyncio.run(main())


