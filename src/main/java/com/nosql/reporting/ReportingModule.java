package com.nosql.reporting;

import java.sql.*;

public class ReportingModule {

    private static String getEnv(String key, boolean required) {
        String value = System.getenv(key);
        if (required && (value == null || value.isEmpty())) {
            throw new RuntimeException("Missing required environment variable: " + key);
        }
        return value;
    }

    public static void main(String[] args) {

        String dbUrl  = getEnv("DB_URL", true);
        String dbUser = getEnv("DB_USER", true);
        String dbPass = getEnv("DB_PASSWORD", true);

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
            run(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void run(Connection conn) throws SQLException {

        String latestRunId = getLatestRunId(conn);
        if (latestRunId == null) {
            System.out.println("No execution metadata found.");
            return;
        }

        System.out.println("==================================================");
        System.out.println("       NASA ETL REPORTING DASHBOARD               ");
        System.out.println("==================================================");

        displayMetadata(conn, latestRunId);
        displayQuery1Results(conn, latestRunId);
        displayQuery2Results(conn, latestRunId);
        displayQuery3Results(conn, latestRunId);
        displayComparativeInsights(conn);
    }

    private static String getLatestRunId(Connection conn) throws SQLException {
        String query = "SELECT run_id FROM execution_metadata ORDER BY execution_time DESC LIMIT 1";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) return rs.getString("run_id");
        }
        return null;
    }

    private static void displayMetadata(Connection conn, String runId) throws SQLException {

        String query = "SELECT * FROM execution_metadata WHERE run_id = ?";

        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, runId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    System.out.println("\n[ EXECUTION METADATA ]");
                    System.out.println("Run ID           : " + rs.getString("run_id"));
                    System.out.println("Pipeline Used    : " + rs.getString("pipeline_name"));
                    System.out.println("Execution Time   : " + rs.getTimestamp("execution_time"));
                    System.out.println("Total Runtime    : " + rs.getLong("runtime_ms") + " ms");
                    System.out.println("Total Batches    : " + rs.getInt("total_batches"));
                    System.out.printf ("Avg Batch Size   : %.2f%n", rs.getDouble("avg_batch_size"));
                    System.out.println("Malformed Records: " + rs.getLong("malformed_record_count"));
                }
            }
        }

        displayGranularQueryMetrics(conn, runId);
    }

    private static void displayGranularQueryMetrics(Connection conn, String runId) throws SQLException {

        System.out.println("\n  --- Granular Query Telemetry ---");

        String sql = "SELECT query_name, runtime_ms FROM query_metrics WHERE run_id = ? ORDER BY query_name ASC";

        try (PreparedStatement ps = prepareStatement(conn, sql, runId);
             ResultSet rs = ps.executeQuery()) {

            boolean hasData = false;

            while (rs.next()) {
                hasData = true;
                System.out.printf("  * %-20s : %d ms%n",
                        rs.getString("query_name"),
                        rs.getLong("runtime_ms"));
            }

            if (!hasData) {
                System.out.println("  * No query telemetry recorded.");
            }
        }
    }

    private static void displayQuery1Results(Connection conn, String runId) throws SQLException {

        String query =
                "SELECT log_date, status_code, request_count, total_bytes " +
                "FROM q1_daily_traffic WHERE run_id = ? " +
                "ORDER BY log_date ASC, status_code ASC " +
                "LIMIT 20";

        try (PreparedStatement ps = prepareStatement(conn, query, runId);
             ResultSet rs = ps.executeQuery()) {

            System.out.println("\n[ QUERY 1: DAILY TRAFFIC SUMMARY (Global) ]");
            System.out.println("--------------------------------------------------------------------");
            System.out.printf("%-15s | %-12s | %-15s | %-15s%n",
                    "Log Date", "Status Code", "Request Count", "Total Bytes");
            System.out.println("--------------------------------------------------------------------");

            while (rs.next()) {
                System.out.printf("%-15s | %-12d | %-15d | %-15d%n",
                        rs.getString("log_date"),
                        rs.getInt("status_code"),
                        rs.getLong("request_count"),
                        rs.getLong("total_bytes"));
            }
        }
    }

    private static void displayQuery2Results(Connection conn, String runId) throws SQLException {

        String query =
                "SELECT resource_path, request_count, total_bytes, distinct_host_count " +
                "FROM q2_top_resources WHERE run_id = ? " +
                "ORDER BY request_count DESC " +
                "LIMIT 20";

        try (PreparedStatement ps = prepareStatement(conn, query, runId);
             ResultSet rs = ps.executeQuery()) {

            System.out.println("\n[ QUERY 2: TOP 20 REQUESTED RESOURCES (Global) ]");
            System.out.println("--------------------------------------------------------------------------------------------");
            System.out.printf("%-40s | %-15s | %-15s | %-15s%n",
                    "Resource Path", "Request Count", "Total Bytes", "Distinct Hosts");
            System.out.println("--------------------------------------------------------------------------------------------");

            while (rs.next()) {
                String path = rs.getString("resource_path");
                if (path != null && path.length() > 38) path = path.substring(0, 35) + "...";

                System.out.printf("%-40s | %-15d | %-15d | %-15d%n",
                        path,
                        rs.getLong("request_count"),
                        rs.getLong("total_bytes"),
                        rs.getLong("distinct_host_count"));
            }
        }
    }

    private static void displayQuery3Results(Connection conn, String runId) throws SQLException {

        String query =
                "SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts " +
                "FROM q3_hourly_errors WHERE run_id = ? " +
                "ORDER BY log_date ASC, log_hour ASC " +
                "LIMIT 20";

        try (PreparedStatement ps = prepareStatement(conn, query, runId);
             ResultSet rs = ps.executeQuery()) {

            System.out.println("\n[ QUERY 3: HOURLY ERROR ANALYSIS (Global) ]");
            System.out.println("--------------------------------------------------------------------------------------------");
            System.out.printf("%-12s | %-6s | %-12s | %-12s | %-10s | %-15s%n",
                    "Log Date", "Hour", "Error Reqs", "Total Reqs", "Error Rate", "Distinct Hosts");
            System.out.println("--------------------------------------------------------------------------------------------");

            while (rs.next()) {
                System.out.printf("%-12s | %-6s | %-12d | %-12d | %-10.4f | %-15d%n",
                        rs.getString("log_date"),
                        rs.getString("log_hour"),
                        rs.getLong("error_request_count"),
                        rs.getLong("total_request_count"),
                        rs.getDouble("error_rate"),
                        rs.getLong("distinct_error_hosts"));
            }
        }
    }

    private static void displayComparativeInsights(Connection conn) throws SQLException {

        String query =
                "SELECT pipeline_name, COUNT(run_id) as total_runs, " +
                "MIN(runtime_ms) as fastest_run_ms, " +
                "AVG(runtime_ms) as avg_run_ms " +
                "FROM execution_metadata " +
                "WHERE runtime_ms > 0 " +
                "GROUP BY pipeline_name " +
                "ORDER BY fastest_run_ms ASC";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            System.out.println("\n=========================================================================================");
            System.out.println("                CROSS-PIPELINE PERFORMANCE LEADERBOARD");
            System.out.println("=========================================================================================");
            System.out.printf("%-15s | %-12s | %-20s | %-20s%n",
                    "Pipeline", "Total Runs", "Fastest Runtime (ms)", "Avg Runtime (ms)");
            System.out.println("-----------------------------------------------------------------------------------------");

            boolean hasData = false;

            while (rs.next()) {
                hasData = true;
                System.out.printf("%-15s | %-12d | %-20d | %-20.2f%n",
                        rs.getString("pipeline_name"),
                        rs.getInt("total_runs"),
                        rs.getLong("fastest_run_ms"),
                        rs.getDouble("avg_run_ms"));
            }

            if (!hasData) System.out.println("No comparative data available yet.");

            System.out.println("=========================================================================================");
        }
    }

    private static PreparedStatement prepareStatement(Connection conn, String sql, String runId) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, runId);
        return ps;
    }
}