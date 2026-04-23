import com.nosql.parser.LogParser;
import com.nosql.parser.NasaLogRecord;
import com.nosql.pipelines.*;
import com.nosql.reporting.ReportingModule; // Added import
import io.github.cdimascio.dotenv.Dotenv;
import java.sql.*;
import java.util.*;
import java.io.*;

public class Main {
    public static void main(String[] args) throws Exception {
        Dotenv dotenv = Dotenv.load();
        Connection conn = DriverManager.getConnection("jdbc:mariadb://localhost:3306/nasa_analytics", 
            dotenv.get("MARIADB_USERNAME"), dotenv.get("MARIADB_PASSWORD"));
        
        // 1. Entry Interface
        System.out.println("=========================================");
        System.out.println("  NASA LOG ANALYTICS - MAIN MENU ");
        System.out.println("=========================================");
        System.out.println("1. Run Pipeline: MongoDB");
        System.out.println("2. Run Pipeline: Pig (Pending Phase 2)");
        System.out.println("3. Run Pipeline: Hive (Pending Phase 2)");
        System.out.println("4. Run Pipeline: MapReduce (Pending Phase 2)");
        System.out.println("5. View Latest Execution Report"); // Added Option 5
        System.out.print("Select an option (1-5): ");
        
        Scanner scanner = new Scanner(System.in);
        int choice = scanner.nextInt();
        
        // Trigger Reporting Module natively
        if (choice == 5) {
            ReportingModule.main(new String[]{});
            scanner.close();
            return;
        }

        Pipeline pipeline;
        if (choice == 1) {
            pipeline = new MongoPipeline(dotenv.get("MONGO_URI"), conn);
        } else if (choice == 4) {
            pipeline = new MapReducePipeline(conn); // Map Option 4!
        } else {
            System.out.println("❌ Pipeline not yet implemented. Exiting.");
            scanner.close();
            return;
        }

        String runId = "RUN_" + System.currentTimeMillis();
        int batchSize = 10000; // Mandatory requirement [cite: 60]
        
        // Initialize Metadata
        PreparedStatement initMeta = conn.prepareStatement(
            "INSERT INTO execution_metadata (run_id, pipeline_name, batch_size, runtime_ms) VALUES (?, ?, ?, ?)");
        initMeta.setString(1, runId);
        initMeta.setString(2, pipeline.getPipelineName());
        initMeta.setInt(3, batchSize);
        initMeta.setLong(4, 0); 
        initMeta.executeUpdate();

        System.out.println("\n🚀 Starting ETL Pipeline: " + runId + " via " + pipeline.getPipelineName());
        long startTime = System.currentTimeMillis(); // Start timer [cite: 64]
        
        LogParser parser = new LogParser();
        List<NasaLogRecord> batch = new ArrayList<>();
        long totalRecords = 0;
        int batchId = 1;

        // Process required log files 
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
        if (!batch.isEmpty()) { // Process final partial batch [cite: 61]
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
        float avgBatchSize = totalBatches > 0 ? (float) totalRecords / totalBatches : 0; // [cite: 62, 63]
        
        PreparedStatement updateMeta = conn.prepareStatement(
            "UPDATE execution_metadata SET batch_id = ?, avg_batch_size = ?, runtime_ms = ? WHERE run_id = ?");
        updateMeta.setInt(1, totalBatches);
        updateMeta.setFloat(2, avgBatchSize);
        updateMeta.setLong(3, runtimeMs);
        updateMeta.setString(4, runId);
        updateMeta.executeUpdate();

        System.out.println("\n✅ Full Dataset Run Complete.");
        System.out.println("Malformed Records: " + parser.getMalformedCount() + " | Total Runtime: " + runtimeMs + "ms"); // [cite: 31, 64]
        scanner.close();
    }
}