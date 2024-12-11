package com.uengine.result_comparator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;



public class LogComparator {
    public static boolean compareLogFiles(String javaLogPath, String plsqlLogPath) throws IOException {
        Map<String, JsonNode> javaLogs = extractCoreInfo(javaLogPath);
        Map<String, JsonNode> plsqlLogs = extractCoreInfo(plsqlLogPath);

        // 로그 출력 (디버깅용)
        System.out.println("Java 로그:");
        javaLogs.forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("\nPL/SQL 로그:");
        plsqlLogs.forEach((k, v) -> System.out.println(k + ": " + v));

        // 비교
        boolean isEqual = javaLogs.equals(plsqlLogs);
        if (!isEqual) {
            System.out.println("\n차이점:");
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode differences = mapper.createObjectNode();

            javaLogs.forEach((key, javaValue) -> {
                JsonNode plsqlValue = plsqlLogs.get(key);
                if (!javaValue.equals(plsqlValue)) {
                    System.out.println("Key: " + key);
                    System.out.println("Java: " + javaValue);
                    System.out.println("PL/SQL: " + (plsqlValue != null ? plsqlValue : "없음"));
                    System.out.println();

                    // Add differences to the JSON object
                    ObjectNode difference = mapper.createObjectNode();
                    difference.set("Java", javaValue);
                    difference.set("PL/SQL", plsqlValue != null ? plsqlValue : mapper.nullNode());
                    differences.set(key, difference);
                }
            });

            // Write differences to a JSON file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/jhyg/Desktop/legacy-modernizer/legacy-modernizer-back/LogComparisonResult.json"))) {
                writer.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(differences));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return isEqual;
    }

    private static Map<String, JsonNode> extractCoreInfo(String logPath) throws IOException {
        Map<String, JsonNode> coreInfo = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(logPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode rootNode = mapper.readTree(line);
                JsonNode payloadNode = rootNode.path("payload");
                
                String operation = payloadNode.path("op").asText();
                String table = payloadNode.path("source").path("table").asText();
                JsonNode afterNode = payloadNode.path("after");

                String key = operation + "_" + table;
                coreInfo.put(key, afterNode);
            }
        }

        return coreInfo;
    }
}