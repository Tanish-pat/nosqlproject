package com.nosql.pipelines;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.nosql.parser.NasaLogRecord;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Sorts.*;

public class MongoPipeline implements Pipeline, AutoCloseable {

    private final MongoClient mongoClient;
    private final Connection dbConn;

    public MongoPipeline(String mongoUri, Connection dbConn) {
        this.mongoClient = MongoClients.create(mongoUri);
        this.dbConn = dbConn;
    }

    @Override
    public String getPipelineName() {
        return "MongoDB";
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    private MongoCollection<Document> getColl(String runId) {
        return mongoClient.getDatabase("nasa_logs").getCollection("raw_" + runId);
    }
    
    @Override
    public void loadBatch(List<NasaLogRecord> batch, String runId, int batchId) {

        List<Document> docs = batch.stream().map(r ->
                new Document("host", r.host)
                        .append("timestamp", r.timestamp)
                        .append("log_date", r.logDate)
                        .append("log_hour", r.logHour)
                        .append("http_method", r.httpMethod)
                        .append("resource_path", r.resourcePath)
                        .append("protocol_version", r.protocolVersion)
                        .append("status_code", r.statusCode)
                        .append("bytes", r.bytesTransferred)
                        .append("batch_id", batchId)
        ).collect(Collectors.toList());

        if (!docs.isEmpty()) {
            getColl(runId).insertMany(docs);
        }
    }

    @Override
    public Map<String, Long> executeQueries(String runId) {

        MongoCollection<Document> coll = getColl(runId);

        Map<String, Long> timings = new LinkedHashMap<>();

        timings.put("Q1_DAILY_TRAFFIC", time(() -> runQ1(coll, runId)));
        timings.put("Q2_TOP_RESOURCES", time(() -> runQ2(coll, runId)));
        timings.put("Q3_HOURLY_ERRORS", time(() -> runQ3(coll, runId)));

        return timings;
    }

    private long time(Runnable r) {
        long s = System.currentTimeMillis();
        r.run();
        return System.currentTimeMillis() - s;
    }

    private void runQ1(MongoCollection<Document> coll, String runId) {

        List<Bson> pipeline = Arrays.asList(
                group(
                    new Document("log_date", "$log_date")
                            .append("status_code", "$status_code"),
                    Accumulators.sum("request_count", 1),
                    Accumulators.sum("total_bytes", "$bytes")
                )
        );

        upload(coll.aggregate(pipeline),
                "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?,?,?,?,?)",
                runId, 1);
    }

    private void runQ2(MongoCollection<Document> coll, String runId) {

        List<Bson> pipeline = Arrays.asList(

                group(
                    new Document("path", "$resource_path")
                            .append("host", "$host"),
                    Accumulators.sum("rc", 1),
                    Accumulators.sum("bc", "$bytes")
                ),

                group(
                    "$_id.path",
                    Accumulators.sum("request_count", "$rc"),
                    Accumulators.sum("total_bytes", "$bc"),
                    Accumulators.sum("distinct_host_count", 1)
                ),

                sort(descending("request_count")),
                limit(20)
        );

        upload(coll.aggregate(pipeline),
                "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?,?,?,?,?)",
                runId, 2);
    }

    private void runQ3(MongoCollection<Document> coll, String runId) {

        List<Bson> pipeline = Arrays.asList(

                group(
                    new Document("date", "$log_date")
                            .append("hour", "$log_hour"),

                    Accumulators.sum("total_request_count", 1),

                    Accumulators.sum("error_request_count",
                            new Document("$cond", Arrays.asList(
                                    new Document("$and", Arrays.asList(
                                            new Document("$gte", Arrays.asList("$status_code", 400)),
                                            new Document("$lte", Arrays.asList("$status_code", 599))
                                    )),
                                    1,
                                    0
                            ))
                    ),

                    Accumulators.addToSet("error_hosts",
                            new Document("$cond", Arrays.asList(
                                    new Document("$and", Arrays.asList(
                                            new Document("$gte", Arrays.asList("$status_code", 400)),
                                            new Document("$lte", Arrays.asList("$status_code", 599))
                                    )),
                                    "$host",
                                    null
                            ))
                    )
                ),

                project(
                    new Document("total_request_count", 1)
                            .append("error_request_count", 1)
                            .append("distinct_error_hosts",
                                    new Document("$size",
                                            new Document("$filter",
                                                    new Document("input", "$error_hosts")
                                                            .append("as", "h")
                                                            .append("cond",
                                                                    new Document("$ne", Arrays.asList("$h", null))
                                                            )
                                            )
                                    )
                            )
                )
        );

        upload(coll.aggregate(pipeline),
                "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?,?,?,?,?,?,?)",
                runId, 3);
    }

    private void upload(AggregateIterable<Document> results, String sql, String runId, int q) {

        try (PreparedStatement ps = dbConn.prepareStatement(sql)) {

            for (Document d : results) {

                ps.setString(1, runId);

                if (q == 1) {
                    Document id = (Document) d.get("_id");

                    ps.setString(2, id.getString("log_date"));
                    ps.setInt(3, id.getInteger("status_code"));
                    ps.setLong(4, ((Number) d.get("request_count")).longValue());
                    ps.setLong(5, ((Number) d.get("total_bytes")).longValue());
                }

                else if (q == 2) {
                    ps.setString(2, d.getString("_id"));
                    ps.setLong(3, ((Number) d.get("request_count")).longValue());
                    ps.setLong(4, ((Number) d.get("total_bytes")).longValue());
                    ps.setLong(5, ((Number) d.get("distinct_host_count")).longValue());
                }

                else {
                    Document id = (Document) d.get("_id");

                    long t = ((Number) d.get("total_request_count")).longValue();
                    long e = ((Number) d.get("error_request_count")).longValue();

                    ps.setString(2, id.getString("date"));
                    ps.setString(3, id.getString("hour"));
                    ps.setLong(4, e);
                    ps.setLong(5, t);
                    ps.setDouble(6, t == 0 ? 0.0 : (double) e / t);
                    ps.setLong(7, ((Number) d.get("distinct_error_hosts")).longValue());
                }

                ps.addBatch();
            }

            ps.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}