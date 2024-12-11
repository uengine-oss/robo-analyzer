package com.uengine.result_comparator;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.*;

class OracleDBManagerTest {
    private static final String PROCEDURE_FILE_PATH = "data/plsql/calculate_payroll.sql";
    private static final String PLSQL_DB_URL = "jdbc:oracle:thin:@localhost:1521/plsqldb";
    private static final String JAVA_DB_URL = "jdbc:oracle:thin:@localhost:1521/javadb";


    //Docker 환경 시작 테스트 (yml 실행 및 초기 오라클 db 설정)
    // @Test
    // void testStartDockerEnvironment() {
    //     assertDoesNotThrow(() -> {
    //         OracleDBManager.startDockerEnvironment();
    //     }, "Docker 환경 시작 중 예외가 발생했습니다.");
    // }


    // 데이터베이스 연결 테스트
    @Test
    void testDatabaseConnection() {
        assertDoesNotThrow(() -> {
            try (Connection conn = OracleDBManager.createDatabaseConnection(PLSQL_DB_URL)) {
                assertTrue(conn.isValid(5));
            }
            try (Connection conn = OracleDBManager.createDatabaseConnection(JAVA_DB_URL)) {
                assertTrue(conn.isValid(5));
            }
        }, "데이터베이스 연결에 실패했습니다.");
    }
    

    // 데이터베이스 초기 설정 초기화 테스트
    @Test
    void testInitializeDatabaseSchema() {
        assertDoesNotThrow(() -> {
            OracleDBManager.dropAllTables(PLSQL_DB_URL);
            OracleDBManager.initializeDatabaseSchema(PLSQL_DB_URL);
            OracleDBManager.enableSupplementalLogging(JAVA_DB_URL);
        }, "데이터베이스 초기 상태 재설정에 실패했습니다.");
    }
    

    // 테이블 데이터 초기화 테스트
    @Test
    void testRefreshData() {
        assertDoesNotThrow(() -> {
            OracleDBManager.refreshData(PLSQL_DB_URL);
            OracleDBManager.refreshData(JAVA_DB_URL);
        }, "데이터베이스 테이블 데이터 초기화에 실패했습니다.");
    }


    // 프로시저 등록 테스트
    @Test
    void testRegisterStoredProcedure() {
        assertDoesNotThrow(() -> {
            OracleDBManager.registerStoredProcedure(PROCEDURE_FILE_PATH, PLSQL_DB_URL);
        }, "프로시저 등록에 실패했습니다.");
    }


    // 프로시저 실행 테스트
    @Test
    void testExecutePayrollCalculation() {
        assertDoesNotThrow(() -> {
            OracleDBManager.executePayrollCalculation(PLSQL_DB_URL);
        }, "급여 계산 프로시저 실행에 실패했습니다.");
    }

    
    // Java 서비스를 통한 급여 계산 테스트
    @Test
    void testCalculatePayrollViaJavaService() {
        assertDoesNotThrow(() -> {
            String result = OracleDBManager.calculatePayrollViaJavaService(1, true, true, true);
            assertNotNull(result);
            assertFalse(result.isEmpty());
            System.out.println("계산 결과: " + result);
        }, "Java 서비스를 통한 급여 계산에 실패했습니다.");
    }

    // TODO 여기까지

    // 트랜잭션 로그가 일치하는지 테스트
    @Test
    void testLogComparison() {
        String javaLogPath = "target/logs/java_logs.jsonl";
        String plsqlLogPath = "target/logs/plsql_logs.jsonl";

        assertDoesNotThrow(() -> {
            boolean result = LogComparator.compareLogFiles(javaLogPath, plsqlLogPath);
            assertTrue(result, "Java와 PL/SQL 로그의 핵심 정보가 일치하지 않습니다.");

            // 결과를 파일에 기록
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultFilePath))) {
                writer.write("{\"logComparisonResult\": " + Boolean.toString(result) + "}");
            }
        }, "로그 비교 중 예외가 발생했습니다.");
    }


    // // 전체 프로세스 실행
    // @Test
    // void testFullProcess() {
    //     assertDoesNotThrow(() -> {
    //         testStartDockerEnvironment();
    //         testInitializeDatabaseSchema();
    //         testRefreshData();
    //         testRegisterStoredProcedure();
    //         testExecutePayrollCalculation();
    //     }, "전체 프로세스 실행에 실패했습니다.");
    // }
}