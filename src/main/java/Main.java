import com.nosql.parser.LogParser;
import com.nosql.parser.NasaLogRecord;
import com.nosql.pipelines.MongoPipeline;
import com.nosql.pipelines.Pipeline;
import io.github.cdimascio.dotenv.Dotenv;
import java.sql.*;
import java.util.*;
import java.io.*;

public class Main {
    public static void main(String[] args) throws Exception {
        Dotenv dotenv = Dotenv.load();
        String mongoUri = dotenv.get("MONGO_URI");
        String dbUser = dotenv.get("MARIADB_USERNAME");
        String dbPass = dotenv.get("MARIADB_PASSWORD");
        
        String runId = "RUN_" + System.currentTimeMillis();
        int batchSize = 5000; // Mandatory requirement [cite: 60]
        
        Connection conn = DriverManager.getConnection("jdbc:mariadb://localhost:3306/nasa_analytics", dbUser, dbPass);
        
        // Use the Interface to ensure equivalence [cite: 35, 70]
        Pipeline pipeline = new MongoPipeline(mongoUri, conn);
        
        PreparedStatement initMeta = conn.prepareStatement(
            "INSERT INTO execution_metadata (run_id, pipeline_name, batch_size, runtime_ms) VALUES (?, ?, ?, ?)");
        initMeta.setString(1, runId);
        initMeta.setString(2, pipeline.getPipelineName());
        initMeta.setInt(3, batchSize);
        initMeta.setLong(4, 0); 
        initMeta.executeUpdate();

        LogParser parser = new LogParser();

        System.out.println("🚀 Starting ETL Pipeline: " + runId);
        System.out.println("⚙️ Execution Engine: " + pipeline.getPipelineName());
        long startTime = System.currentTimeMillis(); // Measured per project rules [cite: 64]
        
        List<NasaLogRecord> batch = new ArrayList<>();
        long totalRecords = 0;
        int batchId = 1;

        try (BufferedReader br = new BufferedReader(new FileReader("data/NASA_access_log_Jul95"))) {
            String line;
            while ((line = br.readLine()) != null) {
                NasaLogRecord record = parser.parseLine(line);
                if (record != null) {
                    batch.add(record);
                    totalRecords++;
                }
                
                if (batch.size() >= batchSize) {
                    pipeline.execute(batch, runId, batchId);
                    // Live Progress Heartbeat
                    System.out.print("\r📦 Progress: " + totalRecords + " records processed (Batch " + batchId + " complete)...");
                    batchId++;
                    batch.clear();
                }
            }
            // Process final partial batch [cite: 61]
            if (!batch.isEmpty()) {
                pipeline.execute(batch, runId, batchId);
                System.out.print("\r📦 Progress: " + totalRecords + " records processed (Final Batch " + batchId + " complete)...");
                batchId++;
            }
        }

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

        System.out.println("\n\n📊 Run Statistics:");
        System.out.println("Total Records Processed: " + totalRecords);
        System.out.println("Malformed Records Count: " + parser.getMalformedCount()); // [cite: 31]
        System.out.println("Total Batches: " + totalBatches);
        System.out.println("Average Batch Size: " + avgBatchSize);
        System.out.println("Total Runtime: " + runtimeMs + "ms");
        System.out.println("✅ Phase 1 Prototype: Data and Metadata successfully stored.");
    }
}