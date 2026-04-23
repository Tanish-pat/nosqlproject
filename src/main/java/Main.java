import com.nosql.parser.LogParser;
import com.nosql.parser.NasaLogRecord;
import com.nosql.pipelines.*;
import com.nosql.reporting.ReportingModule;
import io.github.cdimascio.dotenv.Dotenv;

import java.sql.*;
import java.util.*;
import java.io.*;

public class Main {
    public static void main(String[] args) throws Exception {

        Dotenv dotenv = Dotenv.load();
        Connection conn = DriverManager.getConnection(
                "jdbc:mariadb://localhost:3306/nasa_analytics",
                dotenv.get("MARIADB_USERNAME"),
                dotenv.get("MARIADB_PASSWORD")
        );

        System.out.println("=========================================");
        System.out.println("  NASA LOG ANALYTICS - MAIN MENU ");
        System.out.println("=========================================");
        System.out.println("1. Run Pipeline: MongoDB (DONE!!)");
        System.out.println("2. Run Pipeline: Pig (Pending Phase 2)");
        System.out.println("3. Run Pipeline: Hive (Pending Phase 2)");
        System.out.println("4. Run Pipeline: MapReduce (DONE!!)");
        System.out.println("5. View Latest Execution Report (DONE!!)");
        System.out.print("Select an option (1-5): ");

        try (Scanner scanner = new Scanner(System.in)) {

            int choice = scanner.nextInt();

            // Option 5: Reporting
            if (choice == 5) {
                ReportingModule.main(new String[]{});
                return;
            }

            Pipeline pipeline;

            if (choice == 1) {
                pipeline = new MongoPipeline(dotenv.get("MONGO_URI"), conn);

            } else if (choice == 2) {
                pipeline = new PigPipeline(conn); 

            } else if (choice == 3) {
                String hiveUri = dotenv.get("HIVE_URI");
                String hiveUser = dotenv.get("HIVE_USERNAME");
                String hivePass = dotenv.get("HIVE_PASSWORD");

                if (hiveUri == null) hiveUri = "jdbc:hive2://localhost:10000/default";
                if (hiveUser == null) hiveUser = "";
                if (hivePass == null) hivePass = "";

                pipeline = new HivePipeline(hiveUri, hiveUser, hivePass, conn);

            } else if (choice == 4) {
                pipeline = new MapReducePipeline(conn);

            } else {
                System.out.println("❌ Pipeline not yet implemented. Exiting.");
                return;
            }

            String runId = "RUN_" + System.currentTimeMillis();
            int batchSize = 1000;

            // Initialize Metadata
            PreparedStatement initMeta = conn.prepareStatement(
                    "INSERT INTO execution_metadata (run_id, pipeline_name, batch_size, runtime_ms, query_name) VALUES (?, ?, ?, ?, ?)");
            initMeta.setString(1, runId);
            initMeta.setString(2, pipeline.getPipelineName());
            initMeta.setInt(3, batchSize);
            initMeta.setLong(4, 0);
            initMeta.setString(5, "Q1: Daily Traffic, Q2: Top Resources, Q3: Hourly Errors"); // Explicit Audit Trail
            initMeta.executeUpdate();
            System.out.println("\n🚀 Starting ETL Pipeline: " + runId + " via " + pipeline.getPipelineName());
            long startTime = System.currentTimeMillis();

            LogParser parser = new LogParser();
            List<NasaLogRecord> batch = new ArrayList<>();
            long totalRecords = 0;
            int batchId = 1;

            // String[] targetFiles = {"data/NASA_access_log_Jul95", "data/NASA_access_log_Aug95"};
            String[] targetFiles = {"data/NASA_access_log_sample"};

            System.out.println("--- PHASE 1: DATA INGESTION ---");

            for (String filePath : targetFiles) {
                System.out.println("📖 Reading: " + filePath);

                try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                    String line;

                    while ((line = br.readLine()) != null) {
                        NasaLogRecord record = parser.parseLine(line);

                        if (record != null) {
                            batch.add(record);
                            totalRecords++;
                        }

                        if (batch.size() >= batchSize) {
                            pipeline.loadBatch(batch, runId, batchId);
                            System.out.print("\r📦 Ingested " + totalRecords + " records (Batch " + batchId + ")...");
                            batchId++;
                            batch.clear();
                        }
                    }
                }
            }

            // Final partial batch
            if (!batch.isEmpty()) {
                pipeline.loadBatch(batch, runId, batchId);
                System.out.print("\r📦 Ingested " + totalRecords + " records (Final Batch " + batchId + ")...");
                batchId++;
            }

            System.out.println("\n\n--- PHASE 2: QUERY EXECUTION ---");
            System.out.println("⚙️ Running Q1, Q2, and Q3 aggregations in " + pipeline.getPipelineName() + "...");
            pipeline.executeQueries(runId);

            long endTime = System.currentTimeMillis();
            long runtimeMs = endTime - startTime;

            int totalBatches = batchId - 1;
            float avgBatchSize = totalBatches > 0 ? (float) totalRecords / totalBatches : 0;

            PreparedStatement updateMeta = conn.prepareStatement(
                    "UPDATE execution_metadata SET batch_id = ?, avg_batch_size = ?, runtime_ms = ? WHERE run_id = ?");
            updateMeta.setInt(1, totalBatches);
            updateMeta.setFloat(2, avgBatchSize);
            updateMeta.setLong(3, runtimeMs);
            updateMeta.setString(4, runId);
            updateMeta.executeUpdate();

            System.out.println("\n✅ Full Dataset Run Complete.");
            System.out.println("Malformed Records: " + parser.getMalformedCount()
                    + " | Total Runtime: " + runtimeMs + "ms");
        }

        // Optional: close DB connection explicitly
        conn.close();
    }
}