import com.nosql.parser.LogParser;
import com.nosql.parser.NasaLogRecord;
import com.nosql.pipelines.*;
import com.nosql.reporting.ReportingModule;

import java.sql.*;
import java.util.*;
import java.io.*;

public class Main {

    private static String getEnv(String key, boolean required) {
        String value = System.getenv(key);
        if (required && (value == null || value.isEmpty())) {
            throw new RuntimeException("Missing required environment variable: " + key);
        }
        return value;
    }

    public static void main(String[] args) {

        try {
            String dbUrl  = getEnv("DB_URL", true);
            String dbUser = getEnv("DB_USER", true);
            String dbPass = getEnv("DB_PASSWORD", true);

            String mongoUri = getEnv("MONGO_URI", false);
            String hiveUri  = getEnv("HIVE_URI", false);
            String hiveUser = getEnv("HIVE_USERNAME", false);
            String hivePass = getEnv("HIVE_PASSWORD", false);

            int batchSize = Integer.parseInt(
                    Optional.ofNullable(System.getenv("BATCH_SIZE")).orElse("1000")
            );

            int choice = Integer.parseInt(getEnv("PIPELINE_NUMBER", true));

            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {

                conn.setAutoCommit(false);

                Pipeline pipeline;

                switch (choice) {
                    case 1:
                        if (mongoUri == null || mongoUri.isEmpty())
                            throw new RuntimeException("MONGO_URI not set");
                        pipeline = new MongoPipeline(mongoUri, conn);
                        break;

                    case 2:
                        pipeline = new PigPipeline(conn);
                        break;

                    case 3:
                        if (hiveUri == null || hiveUri.isEmpty())
                            hiveUri = "jdbc:hive2://hive-server:10000/default";
                        pipeline = new HivePipeline(
                                hiveUri,
                                hiveUser != null ? hiveUser : "",
                                hivePass != null ? hivePass : "",
                                conn
                        );
                        break;

                    case 4:
                        pipeline = new MapReducePipeline(conn);
                        break;

                    default:
                        throw new RuntimeException("Invalid PIPELINE_NUMBER. Must be 1–4.");
                }

                String runId = "RUN_" + System.currentTimeMillis();

                try (PreparedStatement initMeta = conn.prepareStatement(
                        "INSERT INTO execution_metadata (run_id, pipeline_name, batch_size, runtime_ms) VALUES (?, ?, ?, ?)"
                )) {
                    initMeta.setString(1, runId);
                    initMeta.setString(2, pipeline.getPipelineName());
                    initMeta.setInt(3, batchSize);
                    initMeta.setLong(4, 0);
                    initMeta.executeUpdate();
                }

                System.out.println("Starting ETL Pipeline: " + runId + " via " + pipeline.getPipelineName());

                long startTime = System.currentTimeMillis();

                LogParser parser = new LogParser();
                List<NasaLogRecord> batch = new ArrayList<>();
                long totalRecords = 0;
                int batchId = 1;

                // Replace with big log set here
                
                String[] targetFiles = {"/app/data/NASA_access_log_sample"};

                for (String filePath : targetFiles) {
                    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            NasaLogRecord record = parser.parseLine(line);
                            if (record != null) {
                                batch.add(record);
                                totalRecords++;
                            }
                            if (batch.size() >= batchSize) {
                                pipeline.loadBatch(batch, runId, batchId++);
                                batch.clear();
                            }
                        }
                    }
                }

                if (!batch.isEmpty()) {
                    pipeline.loadBatch(batch, runId, batchId++);
                }

                int totalBatches = batchId - 1;
                double avgBatchSize = totalBatches > 0 ? (double) totalRecords / totalBatches : 0;

                Map<String, Long> queryTimings = pipeline.executeQueries(runId);

                if (queryTimings != null && !queryTimings.isEmpty()) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO query_metrics (run_id, query_name, runtime_ms) VALUES (?, ?, ?)"
                    )) {
                        for (Map.Entry<String, Long> entry : queryTimings.entrySet()) {
                            ps.setString(1, runId);
                            ps.setString(2, entry.getKey());
                            ps.setLong(3, entry.getValue());
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                }

                long runtimeMs = System.currentTimeMillis() - startTime;

                try (PreparedStatement updateMeta = conn.prepareStatement(
                        "UPDATE execution_metadata SET total_batches = ?, avg_batch_size = ?, malformed_record_count = ?, runtime_ms = ? WHERE run_id = ?"
                )) {
                    updateMeta.setInt(1, totalBatches);
                    updateMeta.setDouble(2, avgBatchSize);
                    updateMeta.setLong(3, parser.getMalformedCount());
                    updateMeta.setLong(4, runtimeMs);
                    updateMeta.setString(5, runId);
                    updateMeta.executeUpdate();
                }

                conn.commit();

                System.out.println("Run Complete.");
                System.out.println("Total Records Parsed : " + parser.getParsedCount());
                System.out.println("Malformed Records    : " + parser.getMalformedCount());
                System.out.println("Total Batches        : " + totalBatches);
                System.out.printf ("Avg Batch Size       : %.2f%n", avgBatchSize);
                System.out.println("Total Runtime        : " + runtimeMs + " ms");

                ReportingModule.run(conn);

                if (pipeline instanceof AutoCloseable) {
                    try { ((AutoCloseable) pipeline).close(); }
                    catch (Exception ex) { ex.printStackTrace(); }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}