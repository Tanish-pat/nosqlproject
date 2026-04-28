package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import org.apache.pig.PigServer;

import java.io.*;
import java.sql.*;
import java.util.*;

public class PigPipeline implements Pipeline {

    private final Connection conn;
    private final String baseDir = "/tmp/nasa_logs_pig/";

    public PigPipeline(Connection conn) {
        this.conn = conn;
    }

    @Override
    public String getPipelineName() {
        return "Pig";
    }

    @Override
    public void loadBatch(List<NasaLogRecord> batch, String runId, int batchId) {

        try {
            File dir = new File(baseDir + runId + "/input/");
            if (!dir.exists()) dir.mkdirs();

            File file = new File(dir, "batch_" + batchId + ".tsv");

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
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

        try {
            PigServer pig = new PigServer("local");

            pig.registerQuery(
                "raw = LOAD 'file://" + baseDir + runId + "/input/' " +
                "USING PigStorage('\\t') AS (" +
                "host:chararray, log_date:chararray, log_hour:chararray, " +
                "resource_path:chararray, status_code:int, bytes:long, batch_id:int);"
            );

            timings.put("Q1_DAILY_TRAFFIC", runQ1(pig, runId));
            timings.put("Q2_TOP_RESOURCES", runQ2(pig, runId));
            timings.put("Q3_HOURLY_ERRORS", runQ3(pig, runId));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return timings;
    }

    private long runQ1(PigServer pig, String runId) throws Exception {

        long start = System.currentTimeMillis();

        pig.registerQuery(
            "g1 = GROUP raw BY (log_date, status_code);" +
            "q1 = FOREACH g1 GENERATE " +
            "  FLATTEN(group) AS (log_date, status_code), " +
            "  COUNT(raw) AS request_count, " +
            "  SUM(raw.bytes) AS total_bytes;"
        );

        store(
            pig,
            "q1",
            baseDir + runId + "/q1_out",
            "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?,?,?,?,?)",
            (ps, v) -> {
                ps.setString(1, runId);
                ps.setString(2, v[0]);
                ps.setInt(3, parseInt(v[1]));
                ps.setLong(4, parseLong(v[2]));
                ps.setLong(5, parseLong(v[3]));
            }
        );

        return System.currentTimeMillis() - start;
    }

    private long runQ2(PigServer pig, String runId) throws Exception {

        long start = System.currentTimeMillis();

        pig.registerQuery(
            "g2a = GROUP raw BY (resource_path, host);" +
            "deduped = FOREACH g2a GENERATE " +
            "  FLATTEN(group) AS (resource_path, host), " +
            "  COUNT(raw) AS rc, " +
            "  SUM(raw.bytes) AS bc;" +

            "g2b = GROUP deduped BY resource_path;" +
            "q2_all = FOREACH g2b GENERATE " +
            "  group AS resource_path, " +
            "  SUM(deduped.rc) AS request_count, " +
            "  SUM(deduped.bc) AS total_bytes, " +
            "  COUNT(deduped) AS distinct_host_count;" +

            "q2_sorted = ORDER q2_all BY request_count DESC;" +
            "q2 = LIMIT q2_sorted 20;"
        );

        store(
            pig,
            "q2",
            baseDir + runId + "/q2_out",
            "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?,?,?,?,?)",
            (ps, v) -> {
                ps.setString(1, runId);
                ps.setString(2, v[0]);
                ps.setLong(3, parseLong(v[1]));
                ps.setLong(4, parseLong(v[2]));
                ps.setLong(5, parseLong(v[3]));
            }
        );

        return System.currentTimeMillis() - start;
    }

    private long runQ3(PigServer pig, String runId) throws Exception {

        long start = System.currentTimeMillis();

        pig.registerQuery(
            "g3 = GROUP raw BY (log_date, log_hour);" +

            "q3 = FOREACH g3 {" +
            "  errs = FILTER raw BY status_code >= 400 AND status_code <= 599;" +
            "  err_hosts = FOREACH errs GENERATE host;" +
            "  distinct_err_hosts = DISTINCT err_hosts;" +
            "  GENERATE " +
            "    FLATTEN(group) AS (log_date, log_hour), " +
            "    COUNT(errs) AS error_request_count, " +
            "    COUNT(raw) AS total_request_count, " +
            "    COUNT(distinct_err_hosts) AS distinct_error_hosts;" +
            "};"
        );

        store(
            pig,
            "q3",
            baseDir + runId + "/q3_out",
            "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?,?,?,?,?,?,?)",
            (ps, v) -> {
                long err = parseLong(v[2]);
                long tot = parseLong(v[3]);

                ps.setString(1, runId);
                ps.setString(2, v[0]);
                ps.setString(3, v[1]);
                ps.setLong(4, err);
                ps.setLong(5, tot);
                ps.setDouble(6, tot == 0 ? 0.0 : (double) err / tot);
                ps.setLong(7, parseLong(v[4]));
            }
        );

        return System.currentTimeMillis() - start;
    }

    private void store(
        PigServer pig,
        String alias,
        String out,
        String sql,
        DBSetter setter
    ) throws Exception {

        pig.store(alias, "file://" + out, "PigStorage('\\t')");

        try (PreparedStatement ps = conn.prepareStatement(sql)) {

            File folder = new File(out);
            File[] files = folder.listFiles((d, name) -> name.startsWith("part-"));

            if (files == null) return;

            for (File f : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] v = line.split("\t", -1);
                        setter.set(ps, v);
                        ps.addBatch();
                    }
                }
            }

            ps.executeBatch();
        }
    }

    @FunctionalInterface
    interface DBSetter {
        void set(PreparedStatement ps, String[] v) throws SQLException;
    }

    private int parseInt(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 0; }
    }

    private long parseLong(String s) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return 0; }
    }

    private String safe(String s) {
        return s == null ? "-" : s;
    }
}