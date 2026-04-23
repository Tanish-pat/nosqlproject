package com.nosql.pipelines;

import com.mongodb.client.*;
import com.nosql.parser.NasaLogRecord;
import org.bson.Document;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class MongoPipeline implements Pipeline {
    private final MongoClient mongoClient;
    private final Connection mysqlConn;

    public MongoPipeline(String mongoUri, Connection mysqlConn) {
        this.mongoClient = MongoClients.create(mongoUri);
        this.mysqlConn = mysqlConn;
    }

    @Override
    public String getPipelineName() { return "MongoDB"; }

    @Override
    public void execute(List<NasaLogRecord> batch, String runId, int batchId) {
        MongoDatabase db = mongoClient.getDatabase("nasa_logs");
        MongoCollection<Document> collection = db.getCollection("raw_batch_" + runId);

        // 1. Load into MongoDB [cite: 72]
        List<Document> docs = batch.stream().map(r -> new Document("host", r.host)
                .append("log_date", r.logDate).append("status_code", r.statusCode)
                .append("bytes", r.bytesTransferred)).collect(Collectors.toList());
        collection.insertMany(docs);

        // 2. Aggregate (Query 1: Daily Traffic)
        String insertSql = "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?)";
        
        try (PreparedStatement ps = mysqlConn.prepareStatement(insertSql)) {
            // Group by logDate AND statusCode per Query 1 requirements [cite: 44]
            Map<String, List<NasaLogRecord>> grouped = batch.stream()
                .collect(Collectors.groupingBy(r -> r.logDate + "|" + r.statusCode));

            for (Map.Entry<String, List<NasaLogRecord>> entry : grouped.entrySet()) {
                String[] keys = entry.getKey().split("\\|");
                String logDate = keys[0];
                int statusCode = Integer.parseInt(keys[1]);
                long requestCount = entry.getValue().size();
                long totalBytes = entry.getValue().stream().mapToLong(r -> r.bytesTransferred).sum(); // Actual byte summation

                ps.setString(1, runId);
                ps.setString(2, logDate);
                ps.setInt(3, statusCode);
                ps.setLong(4, requestCount);
                ps.setLong(5, totalBytes);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) { 
            e.printStackTrace(); 
        }
    }
}