import asyncio
import textwrap
import os, sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from _common import setup_logging, get_context, persist_context

from convert.create_service_preprocessing import start_service_preprocessing
from convert.validate_service_preprocessing import start_validate_service_preprocessing
from convert.create_service_postprocessing import start_service_postprocessing, generate_service_class


async def main() -> None:
    setup_logging()

    ctx = get_context()
    file_names = ctx['file_names']
    user_id = ctx['user_id']
    api_key = ctx['api_key']
    project_name = ctx['project_name']
    locale = ctx['locale']

    used_query_methods = ctx['results'].get('used_query_methods', {})
    sequence_methods = ctx['results'].get('sequence_methods', [])
    service_skeleton_results = ctx['results'].get('service_skeleton_results', {})
    assert service_skeleton_results, '서비스 스켈레톤 결과가 필요합니다. test_3_service_skeleton 실행 후 재시도하세요.'

    outputs = {}

    for _, object_name in file_names:
        skel = service_skeleton_results[object_name]
        service_info = skel['service_info']
        service_skeleton = skel['service_skeleton']
        service_class_name = skel['service_class_name']

        merge_method_code = ''

        for info in service_info:
            variable_nodes, merged_java_code = await start_service_preprocessing(
                info['service_method_skeleton'],
                info['command_class_variable'],
                info['procedure_name'],
                used_query_methods,
                object_name,
                sequence_methods,
                user_id,
                api_key,
                locale,
            )

            await start_validate_service_preprocessing(
                variable_nodes,
                info['service_method_skeleton'],
                info['command_class_variable'],
                info['procedure_name'],
                used_query_methods,
                object_name,
                sequence_methods,
                user_id,
                api_key,
                locale,
            )

            if merged_java_code:
                indented = textwrap.indent(merged_java_code.strip(), '        ')
                completed_service_code = info['method_skeleton_code'].replace('        CodePlaceHolder', 'CodePlaceHolder').replace('CodePlaceHolder', indented)
                merge_method_code = f"{merge_method_code}\n\n{completed_service_code}"
            else:
                merge_method_code = await start_service_postprocessing(
                    info['method_skeleton_code'],
                    info['procedure_name'],
                    object_name,
                    merge_method_code,
                    user_id,
                )

        service_code = await generate_service_class(service_skeleton, service_class_name, merge_method_code, user_id, project_name)

        outputs[object_name] = {
            'service_code': service_code,
            'service_class_name': service_class_name,
        }

    persist_context(ctx, {'service_outputs': outputs})

    print(f"service ok: {len(outputs)}")


if __name__ == '__main__':
    asyncio.run(main())


