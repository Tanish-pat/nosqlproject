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
        System.out.println("  NASA LOG ANALYTICS - PHASE 1 MENU ");
        System.out.println("=========================================");
        System.out.println("1. Run Pipeline: MongoDB");
        System.out.println("2. Run Pipeline: MapReduce");
        System.out.println("3. View Execution Report & Deep Insights");
        System.out.print("Select an option (1-3): ");

        try (Scanner scanner = new Scanner(System.in)) {
            int choice = scanner.nextInt();

            if (choice == 3) {
                ReportingModule.main(new String[]{});
                return;
            }

            Pipeline pipeline;
            if (choice == 1) {
                pipeline = new MongoPipeline(dotenv.get("MONGO_URI"), conn);
            } else if (choice == 2) {
                pipeline = new MapReducePipeline(conn);
            } else {
                System.out.println("❌ Invalid option. Exiting.");
                return;
            }

            String runId = "RUN_" + System.currentTimeMillis();
            int batchSize = 10000;

            // 1. Initialize Parent Metadata (NO query_name here anymore)
            PreparedStatement initMeta = conn.prepareStatement("INSERT INTO execution_metadata (run_id, pipeline_name, batch_size, runtime_ms) VALUES (?, ?, ?, ?)");
            initMeta.setString(1, runId);
            initMeta.setString(2, pipeline.getPipelineName());
            initMeta.setInt(3, batchSize);
            initMeta.setLong(4, 0);
            initMeta.executeUpdate();

            System.out.println("\n🚀 Starting ETL Pipeline: " + runId + " via " + pipeline.getPipelineName());
            
            // Mandatory End-to-End Timer Starts Here
            long startTime = System.currentTimeMillis();

            LogParser parser = new LogParser();
            List<NasaLogRecord> batch = new ArrayList<>();
            long totalRecords = 0;
            int batchId = 1;

            // Use the sample file for rapid testing, but REMEMBER to change this for the final submission!
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

            if (!batch.isEmpty()) {
                pipeline.loadBatch(batch, runId, batchId);
                System.out.print("\r📦 Ingested " + totalRecords + " records (Final Batch " + batchId + ")...");
                batchId++;
            }

            System.out.println("\n\n--- PHASE 2: QUERY EXECUTION ---");
            System.out.println("⚙️ Running Q1, Q2, and Q3 aggregations in " + pipeline.getPipelineName() + "...");
            pipeline.executeQueries(runId);

            // Mandatory End-to-End Timer Ends Here
            long endTime = System.currentTimeMillis();
            long runtimeMs = endTime - startTime;

            int totalBatches = batchId - 1;
            float avgBatchSize = totalBatches > 0 ? (float) totalRecords / totalBatches : 0;

            PreparedStatement updateMeta = conn.prepareStatement("UPDATE execution_metadata SET batch_id = ?, avg_batch_size = ?, runtime_ms = ? WHERE run_id = ?");
            updateMeta.setInt(1, totalBatches);
            updateMeta.setFloat(2, avgBatchSize);
            updateMeta.setLong(3, runtimeMs);
            updateMeta.setString(4, runId);
            updateMeta.executeUpdate();

            System.out.println("\n✅ Full Dataset Run Complete.");
            System.out.println("Malformed Records: " + parser.getMalformedCount() + " | Total Runtime: " + runtimeMs + "ms");
        }
        conn.close();
    }
}