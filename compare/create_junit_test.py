import logging
import os
from util.exception import CompareResultError

TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'


async def create_junit_test(input_parameters: list, camel_object_name: str, procedure_name: str):
    try:
        fields = []
        params = []
        param_string = ""

        # * 파라미터 추출하여 필드 선언 생성 및 파라미터 문자열 생성
        if input_parameters:
            for param in input_parameters:
                fields.append(f"    private {param['javaType']} {param['fieldName']} = {param['value']};")
                params.append(param['fieldName'])
            param_string = ", ".join(params)


        # * object_name과 procedure_name을 각각의 케이스로 전환
        camel_procedure_name = procedure_name.lower().split('_')[0] + ''.join(word.capitalize() for word in procedure_name.lower().split('_')[1:])
        pascal_procedure_name = ''.join(word.capitalize() for word in procedure_name.lower().split('_'))
        camel_procedure_name = pascal_procedure_name[0].lower() + pascal_procedure_name[1:]

        # * 테스트 클래스 템플릿 생성
        test_template = f'''package com.example.demo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import com.example.demo.service.{pascal_procedure_name}Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.File;

import java.util.Date;
import java.util.Base64;
import java.util.Arrays;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;

@SpringBootTest
public class ComparisonTest {{
    
    @Autowired
    private {pascal_procedure_name}Service {camel_procedure_name}Service;

    String javaLogPath = getLogFilePath("java_logs.jsonl");
    String plsqlLogPath = getLogFilePath("plsql_logs.jsonl");
    
    {chr(10).join(fields) if fields else ""}

    @Test
    void testLogComparison() {{
        assertDoesNotThrow(() -> {{
            executeJava();
            boolean result = LogComparator.compareLogFiles(javaLogPath, plsqlLogPath);
            assertTrue(result, "Java와 PL/SQL 로그의 핵심 정보가 일치하지 않습니다.");
        }}, "로그 비교 중 예외가 발생했습니다.");
    }}

    public void executeJava() {{
        {camel_procedure_name}Service.{camel_procedure_name}({param_string});
    }}

    private static ObjectMapper mapper = new ObjectMapper();

    public static boolean compareLogFiles(String javaLogPath, String plsqlLogPath) throws IOException {{
        Map<String, JsonNode> javaLogs = extractCoreInfo(javaLogPath);
        Map<String, JsonNode> plsqlLogs = extractCoreInfo(plsqlLogPath);
        ObjectNode result = mapper.createObjectNode();
        boolean isEqual = true;

        for (Map.Entry<String, JsonNode> entry : javaLogs.entrySet()) {{
            String key = entry.getKey();
            JsonNode javaValue = entry.getValue();
            JsonNode plsqlValue = plsqlLogs.get(key);
            
            ObjectNode comparison = mapper.createObjectNode();
            if (javaValue.equals(plsqlValue)) {{
                comparison.put("status", "identical");
            }} else {{
                isEqual = false;
                comparison.put("status", "different");
                comparison.set("differences", compareData(javaValue, plsqlValue));
            }}
            result.set(key, comparison);
        }}

        // 결과 저장
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String fileName = "compare_result_" + timestamp + ".json";
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(getLogFilePath(fileName)), result);

        return isEqual;
    }}

    private static ObjectNode compareData(JsonNode java, JsonNode plsql) {{
        ObjectNode differences = mapper.createObjectNode();
        for (String section : Arrays.asList("before", "after")) {{
            JsonNode javaSection = java.path(section);
            JsonNode plsqlSection = plsql != null ? plsql.path(section) : mapper.nullNode();
            
            if (!javaSection.equals(plsqlSection)) {{
                ObjectNode diff = mapper.createObjectNode();
                javaSection.fields().forEachRemaining(f -> {{
                    if (!javaSection.path(f.getKey()).equals(plsqlSection.path(f.getKey()))) {{
                        ObjectNode fieldDiff = mapper.createObjectNode();
                        fieldDiff.set("java", f.getValue());
                        fieldDiff.set("plsql", plsqlSection.path(f.getKey()));
                        diff.set(f.getKey(), fieldDiff);
                    }}
                }});
                differences.set(section, diff);
            }}
        }}
        return differences;
    }}


    private static Map<String, JsonNode> extractCoreInfo(String logPath) throws IOException {{
        Map<String, JsonNode> coreInfo = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(logPath))) {{
            String line;
            while ((line = reader.readLine()) != null) {{
                JsonNode rootNode = mapper.readTree(line);
                JsonNode payloadNode = rootNode.path("payload");
                
                // 필드 타입 정보 추출 (한 번만)
                Map<String, String> fieldTypes = new HashMap<>();
                rootNode.path("schema").path("fields").get(0).path("fields")
                    .forEach(field -> {{
                        String fieldName = field.path("field").asText();
                        String fieldType = field.path("name").asText();
                        if (fieldType.isEmpty()) {{
                            fieldType = field.path("type").asText();
                        }}
                        fieldTypes.put(fieldName, fieldType);
                    }});

                // 데이터 디코딩 및 결과 생성
                ObjectNode result = mapper.createObjectNode();
                result.set("fields", mapper.valueToTree(fieldTypes));
                result.set("before", decodeData(payloadNode.path("before"), fieldTypes, mapper));
                result.set("after", decodeData(payloadNode.path("after"), fieldTypes, mapper));
                result.put("table", payloadNode.path("source").path("table").asText());
                result.put("operation", payloadNode.path("op").asText());

                String key = result.get("operation").asText() + "_" + result.get("table").asText();
                coreInfo.put(key, result);

                String logType = logPath.toLowerCase().contains("java") ? "java" : "plsql";
                String fileName = "extracted_" + logType + "_" + key + ".json";
                mapper.writerWithDefaultPrettyPrinter().writeValue(new File(getLogFilePath(fileName)), result);
            }}
        }}
        return coreInfo;
    }}

    private static ObjectNode decodeData(JsonNode dataNode, Map<String, String> fieldTypes, ObjectMapper mapper) {{
        ObjectNode decoded = mapper.createObjectNode();
        if (!dataNode.isMissingNode() && !dataNode.isNull()) {{
            dataNode.fields().forEachRemaining(entry -> {{
                String fieldName = entry.getKey();
                JsonNode value = entry.getValue();
                String type = fieldTypes.get(fieldName);
                decoded.set(fieldName, decodeValue(value, type, mapper));
            }});
        }}
        return decoded;
    }}

    private static JsonNode decodeValue(JsonNode value, String type, ObjectMapper mapper) {{
        if (value.isMissingNode() || value.isNull()) {{
            return value;
        }}

        try {{
            if (type.equals("org.apache.kafka.connect.data.Decimal")) {{
                byte[] bytes = Base64.getDecoder().decode(value.asText());
                return mapper.valueToTree(new BigInteger(bytes).toString());
            }} 
            
            if (type.equals("io.debezium.data.VariableScaleDecimal")) {{
                int scale = value.path("scale").asInt();
                byte[] scaleBytes = Base64.getDecoder().decode(value.path("value").asText());
                BigInteger unscaledValue = new BigInteger(scaleBytes);
                return mapper.valueToTree(new BigDecimal(unscaledValue, scale).toPlainString());
            }}

            if (type.equals("io.debezium.time.Timestamp")) {{
                long epochMillis = value.asLong();
                String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(epochMillis));
                return mapper.valueToTree(dateStr);
            }}
            
            return value;
        }} catch (Exception e) {{
            return value;
        }}
    }}

    private static String getLogFilePath(String fileName) {{
        String currentPath = new File("").getAbsolutePath();
        // demo -> java -> target -> legacy-modernizer 순서로 상위 디렉토리로 이동
        String rootPath = new File(currentPath)
            .getParentFile()  // demo -> java
            .getParentFile()  // java -> target
            .getParentFile()  // target -> legacy-modernizer
            .getAbsolutePath();
        
        return rootPath + File.separator + "legacy-modernizer-back" + File.separator + "logs" + File.separator + fileName;
    }}
    
}}'''
        
        # * 생성된 테스트 코드를 파일로 저장
        parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        test_file_path = os.path.join(parent_workspace_dir, TEST_PATH, "ComparisonTest.java")
        os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, "w", encoding="utf-8") as f:
            f.write(test_template)
        
    except Exception:
        err_msg = "Junit 테스트 코드 작성 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise CompareResultError(err_msg)