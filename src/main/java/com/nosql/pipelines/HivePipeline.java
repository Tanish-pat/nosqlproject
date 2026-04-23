package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import java.io.*;
import java.sql.*;
import java.util.List;

public class HivePipeline implements Pipeline {
    private final String hiveUri;
    private final String hiveUser;
    private final String hivePass;
    private final Connection mysqlConn;
    private final String localBaseDir = "/tmp/nasa_logs_hive/";

    public HivePipeline(String hiveUri, String hiveUser, String hivePass, Connection mysqlConn) {
        this.hiveUri = hiveUri;
        this.hiveUser = hiveUser;
        this.hivePass = hivePass;
        this.mysqlConn = mysqlConn;
    }

    @Override
    public String getPipelineName() { return "Hive"; }

    @Override
    public void loadBatch(List<NasaLogRecord> batch, String runId, int batchId) {
        try {
            File dir = new File(localBaseDir + runId + "/input/");
            if (!dir.exists()) dir.mkdirs();

            File file = new File(dir, "batch_" + batchId + ".tsv");
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
                for (NasaLogRecord r : batch) {
                    // Stage data as Tab-Separated Values for Hive LOAD DATA
                    String line = String.format("%s\t%s\t%s\t%s\t%d\t%d\n", 
                        r.host, r.logDate, r.logHour, r.resourcePath, r.statusCode, r.bytesTransferred);
                    bw.write(line);
                }
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    @Override
    public void executeQueries(String runId) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            try (Connection hiveConn = DriverManager.getConnection(hiveUri, hiveUser, hivePass)) {
                setupHiveTable(hiveConn, runId);

                long t1 = System.currentTimeMillis();
                runQuery1(hiveConn, runId);
                recordQueryMetric(runId, "Q1: Daily Traffic", System.currentTimeMillis() - t1);

                long t2 = System.currentTimeMillis();
                runQuery2(hiveConn, runId);
                recordQueryMetric(runId, "Q2: Top Resources", System.currentTimeMillis() - t2);

                long t3 = System.currentTimeMillis();
                runQuery3(hiveConn, runId);
                recordQueryMetric(runId, "Q3: Hourly Errors", System.currentTimeMillis() - t3);

                cleanupHiveTable(hiveConn, runId);
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            System.err.println("❌ Ensure HiveServer2 is running and accessible at " + hiveUri);
        }
    }

    private void recordQueryMetric(String runId, String queryName, long duration) {
        String sql = "INSERT INTO query_metrics (run_id, query_name, runtime_ms) VALUES (?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            ps.setString(1, runId);
            ps.setString(2, queryName);
            ps.setLong(3, duration);
            ps.executeUpdate();
        } catch (SQLException e) { e.printStackTrace(); }
    }

    private void setupHiveTable(Connection hiveConn, String runId) throws SQLException {
        System.out.println("   -> Setting up Hive tables and loading data...");
        try (Statement stmt = hiveConn.createStatement()) {
            String tableName = "raw_logs_" + runId;
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            
            String createSql = "CREATE TABLE " + tableName + " (" +
                    "host STRING, log_date STRING, log_hour STRING, " +
                    "resource_path STRING, status_code INT, bytes_transferred BIGINT) " +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE";
            stmt.execute(createSql);

            // Load the locally staged directory into the Hive table
            String loadSql = "LOAD DATA LOCAL INPATH '" + localBaseDir + runId + "/input/' INTO TABLE " + tableName;
            stmt.execute(loadSql);
        }
    }

    private void runQuery1(Connection hiveConn, String runId) throws SQLException {
        System.out.println("   -> Executing Hive Query 1 (Daily Traffic)...");
        String hql = "SELECT log_date, status_code, COUNT(*) as request_count, SUM(bytes_transferred) as total_bytes " +
                     "FROM raw_logs_" + runId + " GROUP BY log_date, status_code";
        
        String sql = "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?)";
        try (Statement stmt = hiveConn.createStatement();
             ResultSet rs = stmt.executeQuery(hql);
             PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            while (rs.next()) {
                ps.setString(1, runId);
                ps.setString(2, rs.getString("log_date"));
                ps.setInt(3, rs.getInt("status_code"));
                ps.setLong(4, rs.getLong("request_count"));
                ps.setLong(5, rs.getLong("total_bytes"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void runQuery2(Connection hiveConn, String runId) throws SQLException {
        System.out.println("   -> Executing Hive Query 2 (Top 20 Resources)...");
        String hql = "SELECT resource_path, COUNT(*) as request_count, SUM(bytes_transferred) as total_bytes, " +
                     "COUNT(DISTINCT host) as distinct_host_count " +
                     "FROM raw_logs_" + runId + " GROUP BY resource_path ORDER BY request_count DESC LIMIT 20";

        String sql = "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?)";
        try (Statement stmt = hiveConn.createStatement();
             ResultSet rs = stmt.executeQuery(hql);
             PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            while (rs.next()) {
                ps.setString(1, runId);
                ps.setString(2, rs.getString("resource_path"));
                ps.setLong(3, rs.getLong("request_count"));
                ps.setLong(4, rs.getLong("total_bytes"));
                ps.setLong(5, rs.getLong("distinct_host_count"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void runQuery3(Connection hiveConn, String runId) throws SQLException {
        System.out.println("   -> Executing Hive Query 3 (Hourly Error Analysis)...");
        String hql = "SELECT log_date, log_hour, " +
                     "SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) as error_request_count, " +
                     "COUNT(*) as total_request_count, " +
                     "SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) / COUNT(*) as error_rate, " +
                     "COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host ELSE NULL END) as distinct_error_hosts " +
                     "FROM raw_logs_" + runId + " GROUP BY log_date, log_hour";

        String sql = "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Statement stmt = hiveConn.createStatement();
             ResultSet rs = stmt.executeQuery(hql);
             PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            while (rs.next()) {
                ps.setString(1, runId);
                ps.setString(2, rs.getString("log_date"));
                ps.setString(3, rs.getString("log_hour"));
                ps.setLong(4, rs.getLong("error_request_count"));
                ps.setLong(5, rs.getLong("total_request_count"));
                ps.setFloat(6, rs.getFloat("error_rate"));
                ps.setLong(7, rs.getLong("distinct_error_hosts"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void cleanupHiveTable(Connection hiveConn, String runId) throws SQLException {
        try (Statement stmt = hiveConn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS raw_logs_" + runId);
        }
    }
}