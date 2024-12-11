package com.uengine.result_comparator;

import java.sql.*;
import java.nio.file.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class OracleDBManager {
    // private static final String PLSQL_DB_URL = "jdbc:oracle:thin:@localhost:1521/plsqldb";
    // private static final String JAVA_DB_URL = "jdbc:oracle:thin:@localhost:1521/javadb";
    private static final String USER = "c##debezium";
    private static final String PASS = "dbz";


    /**
     * 트랜잭션 로그 캡처에 필요한 환경을 시작합니다.
     * 
     * @throws IOException Docker Compose 명령 실행 중 I/O 오류 발생 시
     * @throws InterruptedException Docker Compose 프로세스가 중단될 경우
     */
    public static void startDockerEnvironment() throws IOException, InterruptedException {
        executeDockerCompose("docker-compose.yml");
    }


    /**
     * Docker Compose를 사용하여 지정된 YAML 파일의 서비스를 시작합니다.
     * 
     * @param yamlFilePath 실행할 Docker Compose YAML 파일의 경로
     * @throws IOException          Docker Compose 명령 실행 중 I/O 오류 발생 시
     * @throws InterruptedException Docker Compose 프로세스가 중단될 경우
     */
    private static void executeDockerCompose(String yamlFilePath) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("docker-compose", "-f", yamlFilePath, "up", "-d");
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        int exitCode = process.waitFor();
        System.out.println(yamlFilePath + " Docker Compose 실행 완료. 종료 코드: " + exitCode);
    }


    /**
     * 데이터베이스 연결을 생성합니다.
     *
     * @param dbUrl 연결할 데이터베이스의 URL
     * @return 생성된 데이터베이스 연결
     * @throws SQLException 연결 생성 중 오류 발생 시
     */
    public static Connection createDatabaseConnection(String dbUrl) throws SQLException {
        Connection conn = DriverManager.getConnection(dbUrl, USER, PASS);
        conn.setAutoCommit(false);
        return conn;
    }


    /**
     * 새로운 테이블을 생성한 후 보충 로깅을 활성화 합니다.
     *
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException  SQL 파일 읽기 중 오류 발생 시
     */
    public static void initializeDatabaseSchema(String dbUrl) throws SQLException, IOException {
        createAllTables(dbUrl);
        enableSupplementalLogging(dbUrl);
    }


    /**
     * 모든 테이블의 데이터를 삭제하고 초기 샘플 데이터를 삽입합니다.
     *
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException  SQL 파일 읽기 중 오류 발생 시
     */
    public static void refreshData(String dbUrl) throws SQLException, IOException {
        truncateAllTables(dbUrl);
        insertInitialSampleData(dbUrl);
    }


    /**
     * 데이터베이스의 모든 테이블에 대해 보충 로깅을 활성화합니다.
     * 이 메서드는 resetDatabaseToInitialState 메서드와 별도로 실행할 수 있습니다.
     *
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     */
    public static void enableSupplementalLoggingForAllTables(String dbUrl) throws SQLException {
        enableSupplementalLogging(dbUrl);
    }
    

    /**
     * 현재 사용자의 모든 테이블의 데이터를 삭제합니다.
     * 테이블 구조는 유지되며, LOG_MINING_FLUSH 테이블은 제외됩니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     */
    public static void truncateAllTables(String dbUrl) throws SQLException {
        String sql = "BEGIN " +
                     "FOR t IN (SELECT table_name FROM user_tables WHERE table_name != 'LOG_MINING_FLUSH') LOOP " +
                     "EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || t.table_name; " +
                     "END LOOP; " +
                     "END;";
        try (Connection conn = createDatabaseConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            conn.commit();
        }
    }


    /**
     * 현재 사용자의 모든 테이블을 데이터베이스에서 삭제합니다.
     * LOG_MINING_FLUSH 테이블은 제외됩니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     */
    public static void dropAllTables(String dbUrl) throws SQLException {
        String sql = "BEGIN " +
                     "FOR t IN (SELECT table_name FROM user_tables WHERE table_name != 'LOG_MINING_FLUSH') LOOP " +
                     "EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' CASCADE CONSTRAINTS'; " +
                     "END LOOP; " +
                     "END;";
        try (Connection conn = createDatabaseConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            conn.commit();
        }
    }
    

    /**
     * 필요한 모든 테이블을 데이터베이스에 생성합니다.
     * 'create_tables.sql' 스크립트를 실행하여 테이블을 생성합니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException  SQL 파일 읽기 중 오류 발생 시
     */
    public static void createAllTables(String dbUrl) throws SQLException, IOException {
        executeSqlFile("src/main/resources/sample/sql/create_tables.sql", dbUrl);
    }

    
    /**
     * 초기 샘플 데이터를 테이블에 삽입합니다.
     * 'insert_data.sql' 스크립트를 실행하여 데이터를 삽입합니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException  SQL 파일 읽기 중 오류 발생 시
     */
    public static void insertInitialSampleData(String dbUrl) throws SQLException, IOException {
        executeSqlFile("src/main/resources/sample/sql/insert_data.sql", dbUrl);
    }

    
    /**
     * C##DEBEZIUM 스키마의 모든 테이블에 대해 보충 로깅을 설정합니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     */
    public static void enableSupplementalLogging(String dbUrl) throws SQLException {
        String sql = "BEGIN FOR t IN (SELECT owner, table_name FROM all_tables WHERE owner = 'C##DEBEZIUM' AND table_name != 'LOG_MINING_FLUSH') LOOP EXECUTE IMMEDIATE 'ALTER TABLE ' || t.owner || '.' || t.table_name || ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS'; END LOOP; END;";
        try (Connection conn = createDatabaseConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            conn.commit();
        }
    }

    
    /**
     * 지정된 SQL 파일에서 프로시저를 데이터베이스에 등록합니다.
     * 
     * @param filePath 등록할 프로시저가 포함된 SQL 파일의 경로
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException  SQL 파일 읽기 중 오류 발생 시
     */
    public static void registerStoredProcedure(String filePath, String dbUrl) throws SQLException, IOException {
        executeProcedureFile(filePath, dbUrl);
    }

    
    /**
     * 'calculate_payroll' 프로시저를 실행합니다.
     * 이 메서드는 특정 직원에 대한 급여 계산을 수행합니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException 프로시저 실행 중 오류 발생 시
     */
    public static void executePayrollCalculation(String dbUrl) throws SQLException {
        try (Connection conn = createDatabaseConnection(dbUrl);
             CallableStatement stmt = conn.prepareCall("{call calculate_payroll(?, ?, ?, ?)}")) {
            stmt.setInt(1, 1); // employee_id
            stmt.setInt(2, 1); // include_overtime
            stmt.setInt(3, 1); // include_unpaid_leave
            stmt.setInt(4, 1); // include_tax
            stmt.execute();
            conn.commit();
        }
    }

    
    /**
     * 지정된 SQL 파일의 내용을 실행합니다.
     * 
     * @param filePath  실행할 SQL 파일의 경로
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException  SQL 파일 읽기 중 오류 발생 시
     */
    private static void executeSqlFile(String filePath, String dbUrl) throws SQLException, IOException {
        String sqlContent = new String(Files.readAllBytes(Paths.get(filePath)));
        try (Connection conn = createDatabaseConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            for (String sql : sqlContent.split(";")) {
                if (!sql.trim().isEmpty()) {
                    stmt.execute(sql);
                }
            }
            conn.commit();
        }
    }


    /**
     * SQL 파일에 저장된 프로시저를 실행합니다.
     * 이 메소드는 파일의 전체 내용을 하나의 SQL 문으로 간주하고 실행합니다.
     * 주로 CREATE OR REPLACE PROCEDURE와 같은 PL/SQL 블록을 실행하는 데 사용됩니다.
     *
     * @param filePath 실행할 SQL 파일의 경로
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException 파일 읽기 중 오류 발생 시
     */
    public static void executeProcedureFile(String filePath, String dbUrl) throws SQLException, IOException {
        String sqlContent = new String(Files.readAllBytes(Paths.get(filePath)));
        
        try (Connection conn = createDatabaseConnection(dbUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sqlContent);
            conn.commit();
        }
    }


    /**
     * 급여 계산을 위해 Java 서비스로 HTTP POST 요청을 보냅니다.
     *
     * @param employeeId 직원 ID
     * @param includeOvertime 초과 근무 포함 여부
     * @param includeUnpaidLeave 무급 휴가 포함 여부
     * @param includeTax 세금 포함 여부
     * @return 서버 응답
     * @throws IOException 네트워크 오류 발생 시
     * @throws InterruptedException 요청이 중단될 경우
     */
    public static String calculatePayrollViaJavaService(int employeeId, boolean includeOvertime, boolean includeUnpaidLeave, boolean includeTax) throws IOException, InterruptedException {
        String requestBody = String.format("{\"employeeId\":%d,\"includeOvertime\":%b,\"includeUnpaidLeave\":%b,\"includeTax\":%b}",
                employeeId, includeOvertime, includeUnpaidLeave, includeTax);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8082/calculatePayroll"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}