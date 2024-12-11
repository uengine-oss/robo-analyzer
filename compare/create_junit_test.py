import logging
from util.exception import CompareResultError


async def create_junit_test(object_name: str):
    try:
        # 테스트 클래스 템플릿 생성
        test_template = f'''package com.example.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
public class {object_name}ComparisonTest {{
    
    @Autowired
    private {object_name}Service {object_name.lower()}Service;

    String javaLogPath = "src/main/resources/logs/java_logs.jsonl";
    String plsqlLogPath = "src/main/resources/logs/plsql_logs.jsonl";

    @Test
    void testLogComparison() {{
        assertDoesNotThrow(() -> {{
            executeJava();
            boolean result = LogComparator.compareLogFiles(javaLogPath, plsqlLogPath);
            assertTrue(result, "Java와 PL/SQL 로그의 핵심 정보가 일치하지 않습니다.");
        }}, "로그 비교 중 예외가 발생했습니다.");
    }}

    public void executeJava() {{
        {object_name.lower()}Service.{procedure_name}(employeeId, includeOvertime, includeUnpaidLeave, includeTax);
    }}

    public static boolean compareLogFiles(String javaLogPath, String plsqlLogPath) throws IOException {{
        Map<String, JsonNode> javaLogs = extractCoreInfo(javaLogPath);
        Map<String, JsonNode> plsqlLogs = extractCoreInfo(plsqlLogPath);

        // 로그 출력 (디버깅용)
        System.out.println("Java 로그:");
        javaLogs.forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("\nPL/SQL 로그:");
        plsqlLogs.forEach((k, v) -> System.out.println(k + ": " + v));

        // 비교
        boolean isEqual = javaLogs.equals(plsqlLogs);
        if (!isEqual) {{
            System.out.println("\n차이점:");
            javaLogs.forEach((key, javaValue) -> {{
                JsonNode plsqlValue = plsqlLogs.get(key);
                if (!javaValue.equals(plsqlValue)) {{
                    System.out.println("Key: " + key);
                    System.out.println("Java: " + javaValue);
                    System.out.println("PL/SQL: " + (plsqlValue != null ? plsqlValue : "없음"));
                    System.out.println();
                }}
            }});
        }}

        return isEqual;
    }}


    private static Map<String, JsonNode> extractCoreInfo(String logPath) throws IOException {{
        Map<String, JsonNode> coreInfo = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(logPath))) {{
            String line;
            while ((line = reader.readLine()) != null) {{
                JsonNode rootNode = mapper.readTree(line);
                JsonNode payloadNode = rootNode.path("payload");
                
                String operation = payloadNode.path("op").asText();
                String table = payloadNode.path("source").path("table").asText();
                JsonNode afterNode = payloadNode.path("after");

                String key = operation + "_" + table;
                coreInfo.put(key, afterNode);
            }}
        }}

        return coreInfo;
    }}
    
}}'''
        
        # 생성된 테스트 코드를 파일로 저장
        test_file_path = f"test/java/{object_name}ComparisonTest.java"
        with open(test_file_path, "w") as f:
            f.write(test_template)
            
        return test_file_path
        
    except Exception:
        err_msg = "Junit 테스트 코드 작성 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise CompareResultError(err_msg)