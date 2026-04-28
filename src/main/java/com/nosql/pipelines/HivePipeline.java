package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;

import java.io.*;
import java.sql.*;
import java.util.*;

public class HivePipeline implements Pipeline {

    private final String hiveUri, hiveUser, hivePass;
    private final Connection pgConn;
    private final String baseDir = "/tmp/nasa_logs_hive/";

    public HivePipeline(String uri, String user, String pass, Connection pgConn) {
        this.hiveUri = uri;
        this.hiveUser = user;
        this.hivePass = pass;
        this.pgConn = pgConn;
    }

    @Override
    public String getPipelineName() {
        return "Hive";
    }

    @Override
    public void loadBatch(List<NasaLogRecord> batch, String runId, int batchId) {

        try {
            File dir = new File(baseDir + runId + "/input/");
            if (!dir.exists()) dir.mkdirs();

            File out = new File(dir, "batch_" + batchId + ".tsv");

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(out))) {
                for (NasaLogRecord r : batch) {
                    bw.write(
                        safe(r.host) + "\t" +
                        safe(r.logDate) + "\t" +
                        safe(r.logHour) + "\t" +
                        safe(r.resourcePath) + "\t" +
                        r.statusCode + "\t" +
                        r.bytesTransferred + "\t" +
                        batchId + "\n"
                    );
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Long> executeQueries(String runId) {

        Map<String, Long> timings = new LinkedHashMap<>();
        String table = "raw_logs_" + runId.toLowerCase().replaceAll("[^a-z0-9_]", "_");

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            try (Connection hiveConn = DriverManager.getConnection(hiveUri, hiveUser, hivePass);
                 Statement stmt = hiveConn.createStatement()) {

                stmt.execute("DROP TABLE IF EXISTS " + table);

                stmt.execute(
                    "CREATE TABLE " + table + " (" +
                    "host STRING, log_date STRING, log_hour STRING, " +
                    "resource_path STRING, status_code INT, bytes BIGINT, batch_id INT) " +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE"
                );

                stmt.execute(
                    "LOAD DATA LOCAL INPATH '" + baseDir + runId + "/input/' INTO TABLE " + table
                );

                timings.put("Q1_DAILY_TRAFFIC", runQ1(hiveConn, table, runId));
                timings.put("Q2_TOP_RESOURCES", runQ2(hiveConn, table, runId));
                timings.put("Q3_HOURLY_ERRORS", runQ3(hiveConn, table, runId));

                stmt.execute("DROP TABLE IF EXISTS " + table);
                deleteDir(new File(baseDir + runId));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return timings;
    }

    private long runQ1(Connection hive, String table, String runId) throws SQLException {

        String hql =
            "SELECT log_date, status_code, " +
            "COUNT(*) AS request_count, SUM(bytes) AS total_bytes " +
            "FROM " + table + " " +
            "GROUP BY log_date, status_code";

        return runAndLoad(
            hive, runId, hql,
            "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?,?,?,?,?)",
            (ps, rs) -> {
                ps.setString(1, runId);
                ps.setString(2, rs.getString("log_date"));
                ps.setInt(3, rs.getInt("status_code"));
                ps.setLong(4, rs.getLong("request_count"));
                ps.setLong(5, rs.getLong("total_bytes"));
            }
        );
    }

    private long runQ2(Connection hive, String table, String runId) throws SQLException {

        String hql =
            "SELECT resource_path, " +
            "COUNT(*) AS request_count, " +
            "SUM(bytes) AS total_bytes, " +
            "COUNT(DISTINCT host) AS distinct_host_count " +
            "FROM " + table + " " +
            "GROUP BY resource_path " +
            "ORDER BY request_count DESC " +
            "LIMIT 20";

        return runAndLoad(
            hive, runId, hql,
            "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?,?,?,?,?)",
            (ps, rs) -> {
                ps.setString(1, runId);
                ps.setString(2, rs.getString("resource_path"));
                ps.setLong(3, rs.getLong("request_count"));
                ps.setLong(4, rs.getLong("total_bytes"));
                ps.setLong(5, rs.getLong("distinct_host_count"));
            }
        );
    }

    private long runQ3(Connection hive, String table, String runId) throws SQLException {

        String hql =
            "SELECT log_date, log_hour, " +
            "SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS error_count, " +
            "COUNT(*) AS total_count, " +
            "CAST(SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS error_rate, " +
            "COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END) AS distinct_error_hosts " +
            "FROM " + table + " " +
            "GROUP BY log_date, log_hour";

        return runAndLoad(
            hive, runId, hql,
            "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?,?,?,?,?,?,?)",
            (ps, rs) -> {
                ps.setString(1, runId);
                ps.setString(2, rs.getString("log_date"));
                ps.setString(3, rs.getString("log_hour"));
                ps.setLong(4, rs.getLong("error_count"));
                ps.setLong(5, rs.getLong("total_count"));
                ps.setDouble(6, rs.getDouble("error_rate"));
                ps.setLong(7, rs.getLong("distinct_error_hosts"));
            }
        );
    }

    private long runAndLoad(
        Connection hive,
        String runId,
        String hql,
        String sql,
        ResultMapper mapper
    ) throws SQLException {

        long start = System.currentTimeMillis();

        try (Statement stmt = hive.createStatement();
             ResultSet rs = stmt.executeQuery(hql);
             PreparedStatement ps = pgConn.prepareStatement(sql)) {

            while (rs.next()) {
                mapper.map(ps, rs);
                ps.addBatch();
            }

            ps.executeBatch();
        }

        return System.currentTimeMillis() - start;
    }

    @FunctionalInterface
    interface ResultMapper {
        void map(PreparedStatement ps, ResultSet rs) throws SQLException;
    }

    private void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) deleteDir(f);
        }
        dir.delete();
    }

    private String safe(String s) {
        return s == null ? "-" : s;
    }
}