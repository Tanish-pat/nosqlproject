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
    public void loadBatch(List<NasaLogRecord> batch, String runId, int batchId) {
        MongoCollection<Document> collection = mongoClient.getDatabase("nasa_logs").getCollection("raw_batch_" + runId);
        List<Document> docs = batch.stream().map(r -> new Document("host", r.host)
                .append("log_date", r.logDate).append("log_hour", r.logHour)
                .append("resource_path", r.resourcePath).append("status_code", r.statusCode)
                .append("bytes", r.bytesTransferred)).collect(Collectors.toList());
        collection.insertMany(docs);
    }

    @Override
    public void executeQueries(String runId) {
        try {
            MongoCollection<Document> collection = mongoClient.getDatabase("nasa_logs").getCollection("raw_batch_" + runId);
            long t1 = System.currentTimeMillis();
            runQuery1(collection, runId);
            recordQueryMetric(runId, "Q1: Daily Traffic", System.currentTimeMillis() - t1);
            
            long t2 = System.currentTimeMillis();
            runQuery2(collection, runId);
            recordQueryMetric(runId, "Q2: Top Resources", System.currentTimeMillis() - t2);
            
            long t3 = System.currentTimeMillis();
            runQuery3(collection, runId);
            recordQueryMetric(runId, "Q3: Hourly Errors", System.currentTimeMillis() - t3);
        } catch (Exception e) { e.printStackTrace(); }
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
    
    private void runQuery1(MongoCollection<Document> collection, String runId) {
        System.out.println("   -> Executing Query 1 (Daily Traffic)...");
        List<Document> pipeline = Arrays.asList(
            new Document("$group", new Document("_id", new Document("log_date", "$log_date").append("status_code", "$status_code"))
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes")))
        );

        String sql = "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (Document doc : collection.aggregate(pipeline)) {
                Document id = (Document) doc.get("_id");
                ps.setString(1, runId);
                ps.setString(2, id.getString("log_date"));
                ps.setInt(3, id.getInteger("status_code"));
                ps.setLong(4, ((Number) doc.get("request_count")).longValue());
                ps.setLong(5, ((Number) doc.get("total_bytes")).longValue());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) { e.printStackTrace(); }
    }

    private void runQuery2(MongoCollection<Document> collection, String runId) {
        System.out.println("   -> Executing Query 2 (Top 20 Resources)...");
        List<Document> pipeline = Arrays.asList(
            new Document("$group", new Document("_id", "$resource_path")
                .append("request_count", new Document("$sum", 1))
                .append("total_bytes", new Document("$sum", "$bytes"))
                .append("distinct_hosts", new Document("$addToSet", "$host"))),
            new Document("$project", new Document("request_count", 1)
                .append("total_bytes", 1)
                .append("distinct_host_count", new Document("$size", "$distinct_hosts"))),
            new Document("$sort", new Document("request_count", -1)),
            new Document("$limit", 20)
        );

        String sql = "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (Document doc : collection.aggregate(pipeline)) {
                ps.setString(1, runId);
                ps.setString(2, doc.getString("_id"));
                ps.setLong(3, ((Number) doc.get("request_count")).longValue());
                ps.setLong(4, ((Number) doc.get("total_bytes")).longValue());
                ps.setLong(5, ((Number) doc.get("distinct_host_count")).longValue());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) { e.printStackTrace(); }
    }

    private void runQuery3(MongoCollection<Document> collection, String runId) {
        System.out.println("   -> Executing Query 3 (Hourly Error Analysis)...");
        
        // MongoDB aggregation using $cond to check if status code is between 400 and 599
        List<Document> pipeline = Arrays.asList(
            new Document("$group", new Document("_id", new Document("log_date", "$log_date").append("log_hour", "$log_hour"))
                .append("total_request_count", new Document("$sum", 1))
                .append("error_request_count", new Document("$sum", 
                    new Document("$cond", Arrays.asList(
                        new Document("$and", Arrays.asList(
                            new Document("$gte", Arrays.asList("$status_code", 400)),
                            new Document("$lte", Arrays.asList("$status_code", 599))
                        )), 1, 0
                    ))
                ))
                .append("error_hosts_set", new Document("$addToSet", 
                    new Document("$cond", Arrays.asList(
                        new Document("$and", Arrays.asList(
                            new Document("$gte", Arrays.asList("$status_code", 400)),
                            new Document("$lte", Arrays.asList("$status_code", 599))
                        )), "$host", null
                    ))
                ))
            )
        );

        String sql = "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (Document doc : collection.aggregate(pipeline)) {
                Document id = (Document) doc.get("_id");
                
                long totalReq = ((Number) doc.get("total_request_count")).longValue();
                long errorReq = ((Number) doc.get("error_request_count")).longValue();
                
                // Clean up the dynamically aggregated array (remove the 'null' entries for non-error hosts)
                List<String> errorHosts = doc.getList("error_hosts_set", String.class);
                long distinctErrorHosts = 0;
                if (errorHosts != null) {
                    errorHosts.remove(null); 
                    distinctErrorHosts = errorHosts.size();
                }
                
                float errorRate = totalReq == 0 ? 0 : (float) errorReq / totalReq;

                ps.setString(1, runId);
                ps.setString(2, id.getString("log_date"));
                ps.setString(3, id.getString("log_hour"));
                ps.setLong(4, errorReq);
                ps.setLong(5, totalReq);
                ps.setFloat(6, errorRate);
                ps.setLong(7, distinctErrorHosts);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) { e.printStackTrace(); }
    }
}