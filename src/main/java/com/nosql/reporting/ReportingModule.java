package com.nosql.reporting;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.*;

public class ReportingModule {

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.load();
        String dbUser = dotenv.get("MARIADB_USERNAME");
        String dbPass = dotenv.get("MARIADB_PASSWORD");
        String dbUrl = "jdbc:mariadb://localhost:3306/nasa_analytics";

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
            // 1. Get the most recent run_id
            String latestRunId = getLatestRunId(conn);
            if (latestRunId == null) {
                System.out.println("No execution metadata found in the database.");
                return;
            }

            System.out.println("==================================================");
            System.out.println("           NASA ETL REPORTING DASHBOARD           ");
            System.out.println("==================================================");

            // 2. Display Execution Metadata [cite: 39]
            displayMetadata(conn, latestRunId);

            // 3. Display Query 1 Results [cite: 8, 38]
            displayQuery1Results(conn, latestRunId);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static String getLatestRunId(Connection conn) throws SQLException {
        String query = "SELECT run_id FROM execution_metadata ORDER BY execution_time DESC LIMIT 1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getString("run_id");
            }
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
                    System.out.println("Total Batches    : " + rs.getInt("batch_id"));
                    System.out.println("Batch Size       : " + rs.getInt("batch_size"));
                    System.out.printf("Avg Batch Size   : %.2f\n", rs.getFloat("avg_batch_size"));
                }
            }
        }
    }

    private static void displayQuery1Results(Connection conn, String runId) throws SQLException {
        // Fetching top 15 results so it doesn't flood the terminal
        String query = "SELECT log_date, status_code, request_count, total_bytes " +
                       "FROM q1_daily_traffic WHERE run_id = ? " +
                       "ORDER BY log_date ASC, status_code ASC LIMIT 15";
        
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                System.out.println("\n[ QUERY 1: DAILY TRAFFIC SUMMARY (Sample) ]");
                System.out.println("---------------------------------------------------------------");
                System.out.printf("%-15s | %-12s | %-15s | %-15s\n", "Log Date", "Status Code", "Request Count", "Total Bytes");
                System.out.println("---------------------------------------------------------------");
                
                while (rs.next()) {
                    String date = rs.getString("log_date");
                    int status = rs.getInt("status_code");
                    long reqCount = rs.getLong("request_count");
                    long bytes = rs.getLong("total_bytes");
                    
                    System.out.printf("%-15s | %-12d | %-15d | %-15d\n", date, status, reqCount, bytes);
                }
                System.out.println("---------------------------------------------------------------");
                System.out.println("* Showing first 15 rows for preview *");
            }
        }
    }
}