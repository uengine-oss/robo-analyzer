import os, sys, asyncio
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from _common import setup_logging, get_context, persist_context

from convert.create_controller_skeleton import start_controller_skeleton_processing
from convert.create_controller import start_controller_processing, generate_controller_class


async def main() -> None:
    setup_logging()

    ctx = get_context()
    file_names = ctx['file_names']
    user_id = ctx['user_id']
    api_key = ctx['api_key']
    project_name = ctx['project_name']
    locale = ctx['locale']

    service_skeleton_results = ctx['results'].get('service_skeleton_results', {})
    assert service_skeleton_results, '서비스 스켈레톤 결과가 필요합니다. test_3_service_skeleton 실행 후 재시도하세요.'

    outputs = {}

    for _, object_name in file_names:
        skel = service_skeleton_results[object_name]
        service_info = skel['service_info']
        exist_command_class = skel['exist_command_class']

        controller_skeleton, controller_class_name = await start_controller_skeleton_processing(
            object_name, exist_command_class, project_name
        )

        merge_controller_method_code = ''
        for info in service_info:
            merge_controller_method_code = await start_controller_processing(
                info['method_signature'],
                info['procedure_name'],
                info['command_class_variable'],
                info['command_class_name'],
                info['node_type'],
                merge_controller_method_code,
                controller_skeleton,
                object_name,
                user_id,
                api_key,
                project_name,
                locale,
            )

        controller_code = await generate_controller_class(controller_skeleton, controller_class_name, merge_controller_method_code, user_id, project_name)

        outputs[object_name] = {
            'controller_code': controller_code,
            'controller_class_name': controller_class_name,
        }

    persist_context(ctx, {'controller_outputs': outputs})

    print(f"controller ok: {len(outputs)}")


if __name__ == '__main__':
    asyncio.run(main())


