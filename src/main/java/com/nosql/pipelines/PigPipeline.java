package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import org.apache.pig.PigServer;
import java.io.*;
import java.sql.*;
import java.util.List;

public class PigPipeline implements Pipeline {
    private final Connection mysqlConn;
    private final String baseDir = "/tmp/nasa_logs_pig/";

    public PigPipeline(Connection mysqlConn) {
        this.mysqlConn = mysqlConn;
    }

    @Override
    public String getPipelineName() { return "Pig"; }

    @Override
    public void loadBatch(List<NasaLogRecord> batch, String runId, int batchId) {
        try {
            File dir = new File(baseDir + runId + "/input/");
            if (!dir.exists()) dir.mkdirs();

            File file = new File(dir, "batch_" + batchId + ".tsv");
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
                for (NasaLogRecord r : batch) {
                    // Pig reads TSV files identically to MapReduce and Hive
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
            System.out.println("   -> Starting Apache Pig (Local Mode)...");
            
            // Execute Pig in local mode (no Hadoop cluster required)
            PigServer pigServer = new PigServer("local");

            String inputDir = baseDir + runId + "/input/";
            String q1Out = baseDir + runId + "/q1_out";
            String q2Out = baseDir + runId + "/q2_out";
            String q3Out = baseDir + runId + "/q3_out";

            // 0. Load Data into Pig
            pigServer.registerQuery("raw = LOAD 'file://" + inputDir + "' USING PigStorage('\\t') " +
                "AS (host:chararray, log_date:chararray, log_hour:chararray, resource_path:chararray, status_code:int, bytes:long);");

            // 1. Query 1: Daily Traffic Summary
            System.out.println("   -> Executing Pig Job: Query 1 (Daily Traffic)...");
            pigServer.registerQuery("q1_grp = GROUP raw BY (log_date, status_code);");
            pigServer.registerQuery("q1_out = FOREACH q1_grp GENERATE FLATTEN(group) AS (log_date, status_code), " +
                "COUNT(raw) AS req_count, SUM(raw.bytes) AS total_bytes;");
            pigServer.store("q1_out", "file://" + q1Out, "PigStorage('\\t')");

            // 2. Query 2: Top Requested Resources
            System.out.println("   -> Executing Pig Job: Query 2 (Top Resources)...");
            pigServer.registerQuery("q2_grp = GROUP raw BY resource_path;");
            pigServer.registerQuery("q2_agg = FOREACH q2_grp { " +
                "unique_hosts = DISTINCT raw.host; " +
                "GENERATE group AS resource_path, COUNT(raw) AS req_count, SUM(raw.bytes) AS total_bytes, COUNT(unique_hosts) AS distinct_host_count; };");
            pigServer.registerQuery("q2_sorted = ORDER q2_agg BY req_count DESC;");
            pigServer.registerQuery("q2_limit = LIMIT q2_sorted 20;");
            pigServer.store("q2_limit", "file://" + q2Out, "PigStorage('\\t')");

            // 3. Query 3: Hourly Error Analysis
            System.out.println("   -> Executing Pig Job: Query 3 (Hourly Errors)...");
            pigServer.registerQuery("q3_grp = GROUP raw BY (log_date, log_hour);");
            pigServer.registerQuery("q3_agg = FOREACH q3_grp { " +
                "errors = FILTER raw BY (status_code >= 400 AND status_code <= 599); " +
                "unique_err_hosts = DISTINCT errors.host; " +
                "GENERATE FLATTEN(group) AS (log_date, log_hour), " +
                "COUNT(errors) AS error_request_count, COUNT(raw) AS total_request_count, " +
                "((float)COUNT(errors) / (float)COUNT(raw)) AS error_rate, COUNT(unique_err_hosts) AS distinct_error_hosts; };");
            pigServer.store("q3_agg", "file://" + q3Out, "PigStorage('\\t')");

            // Load results back into MariaDB
            System.out.println("   -> Loading Pig results into MariaDB...");
            loadQ1ToDB(q1Out, runId);
            loadQ2ToDB(q2Out, runId);
            loadQ3ToDB(q3Out, runId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadQ1ToDB(String outDir, String runId) throws Exception {
        String sql = "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (File f : new File(outDir).listFiles((d, name) -> name.startsWith("part-"))) {
                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] vals = line.split("\t");
                        ps.setString(1, runId);
                        ps.setString(2, vals[0]);
                        ps.setInt(3, Integer.parseInt(vals[1]));
                        ps.setLong(4, Long.parseLong(vals[2]));
                        ps.setLong(5, vals.length > 3 && !vals[3].isEmpty() ? Long.parseLong(vals[3]) : 0);
                        ps.addBatch();
                    }
                }
            }
            ps.executeBatch();
        }
    }

    private void loadQ2ToDB(String outDir, String runId) throws Exception {
        String sql = "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (File f : new File(outDir).listFiles((d, name) -> name.startsWith("part-"))) {
                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] vals = line.split("\t");
                        ps.setString(1, runId);
                        ps.setString(2, vals[0]);
                        ps.setLong(3, Long.parseLong(vals[1]));
                        ps.setLong(4, vals.length > 2 && !vals[2].isEmpty() ? Long.parseLong(vals[2]) : 0);
                        ps.setLong(5, vals.length > 3 && !vals[3].isEmpty() ? Long.parseLong(vals[3]) : 0);
                        ps.addBatch();
                    }
                }
            }
            ps.executeBatch();
        }
    }

    private void loadQ3ToDB(String outDir, String runId) throws Exception {
        String sql = "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (File f : new File(outDir).listFiles((d, name) -> name.startsWith("part-"))) {
                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] vals = line.split("\t");
                        ps.setString(1, runId);
                        ps.setString(2, vals[0]);
                        ps.setString(3, vals[1]);
                        ps.setLong(4, Long.parseLong(vals[2]));
                        ps.setLong(5, Long.parseLong(vals[3]));
                        ps.setFloat(6, Float.parseFloat(vals[4]));
                        ps.setLong(7, vals.length > 5 && !vals[5].isEmpty() ? Long.parseLong(vals[5]) : 0);
                        ps.addBatch();
                    }
                }
            }
            ps.executeBatch();
        }
    }
}