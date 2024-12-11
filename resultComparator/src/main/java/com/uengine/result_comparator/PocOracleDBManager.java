package com.uengine.result_comparator;

import java.sql.*;
import java.nio.file.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Stream;


public class PocOracleDBManager {
    private static final String USER = "c##debezium";
    private static final String PASS = "dbz";


    /**
     * 트랜잭션 로그 캡처에 필요한 환경을 시작합니다.
     * 
     * @throws IOException Docker Compose 명령 실행 중 I/O 오류 발생 시
     * @throws InterruptedException Docker Compose 프로세스가 중단될 경우
     */
    public static void startDockerEnvironment() throws IOException, InterruptedException {
        executeDockerCompose("poc-oracleDB.yml");
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
     * TPJ로 시작하는 sql 스크립트를 실행하여 테이블을 생성합니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException  SQL 파일 읽기 중 오류 발생 시
     */
    public static void createAllTables(String dbUrl) throws SQLException, IOException {
        Path sqlDir = Paths.get("src/main/resources/poc/sql");
        try (Stream<Path> paths = Files.walk(sqlDir)) {
            paths.filter(Files::isRegularFile)
                 .filter(path -> path.getFileName().toString().startsWith("TPJ_"))
                 .forEach(path -> {
                     try {
                         executeSqlFile(path.toString(), dbUrl);
                         System.out.println(path.getFileName() + " 실행 완료");
                     } catch (Exception e) {
                         System.err.println(path.getFileName() + " 실행 중 오류: " + e.getMessage());
                     }
                 });
        }
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
        executeSqlFile("src/main/resources/poc/sql/insert_data.sql", dbUrl);
    }

    
    /**
     * TPX_로 시작하는 모든 프로시저 파일들을 등록합니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException 파일 읽기 중 오류 발생 시
     */
    public static void registerAllProcedures(String dbUrl) throws SQLException, IOException {
        Path sqlDir = Paths.get("src/main/resources/poc/result");
        try (Stream<Path> paths = Files.walk(sqlDir)) {
            paths.filter(Files::isRegularFile)
                 .filter(path -> path.getFileName().toString().startsWith("TPX_"))
                 .forEach(path -> {
                     try {
                         executeProcedureFile(path.toString(), dbUrl);
                         System.out.println(path.getFileName() + " 프로시저 등록 완료");
                     } catch (Exception e) {
                         System.err.println(path.getFileName() + " 프로시저 등록 중 오류: " + e.getMessage());
                     }
                 });
        }
    }


    /**
     * 테이블스페이스를 생성합니다.
     * 
     * @param dbUrl 연결할 데이터베이스의 URL
     * @throws SQLException SQL 실행 중 오류 발생 시
     * @throws IOException SQL 파일 읽기 중 오류 발생 시
     */
    public static void createTableSpace(String dbUrl) throws SQLException, IOException {
        executeSqlFile("src/main/resources/poc/sql/create_table_space.sql", dbUrl);
    }

    
    public static void setNativePLSQLMode(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER SESSION SET PLSQL_CODE_TYPE = 'NATIVE'");
        } catch (SQLException e) {
            System.err.println("PL/SQL 네이티브 모드 설정 중 오류: " + e.getMessage());
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
     * SQL 파일에 저장된 프로시저를 등록(컴파일)합니다.
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
}