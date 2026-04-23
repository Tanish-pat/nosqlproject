package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.sql.*;
import java.util.*;

public class MapReducePipeline implements Pipeline {
    private final Connection mysqlConn;
    private final String hdfsBaseDir = "/tmp/nasa_logs_mr/";

    public MapReducePipeline(Connection mysqlConn) {
        this.mysqlConn = mysqlConn;
    }

    @Override
    public String getPipelineName() { return "MapReduce"; }

    @Override
    public void loadBatch(List<NasaLogRecord> batch, String runId, int batchId) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.getLocal(conf);
            Path batchPath = new Path(hdfsBaseDir + runId + "/input/batch_" + batchId + ".tsv");
            
            try (FSDataOutputStream out = fs.create(batchPath)) {
                for (NasaLogRecord r : batch) {
                    // Stage data as Tab-Separated Values (TSV) for Hadoop
                    String line = String.format("%s\t%s\t%s\t%s\t%d\t%d\n", 
                        r.host, r.logDate, r.logHour, r.resourcePath, r.statusCode, r.bytesTransferred);
                    out.writeBytes(line);
                }
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    @Override
    public void executeQueries(String runId) {
        try {
            runQuery1(runId);
            runQuery2(runId);
            runQuery3(runId);
        } catch (Exception e) { e.printStackTrace(); }
    }

    // ==========================================
    // QUERY 1: DAILY TRAFFIC (MapReduce)
    // ==========================================
    public static class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 6) {
                // Key: logDate|statusCode, Value: 1|bytes
                context.write(new Text(parts[1] + "|" + parts[4]), new Text("1|" + parts[5]));
            }
        }
    }

    public static class Q1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long reqCount = 0;
            long totalBytes = 0;
            for (Text val : values) {
                String[] p = val.toString().split("\\|");
                reqCount += Long.parseLong(p[0]);
                totalBytes += Long.parseLong(p[1]);
            }
            context.write(key, new Text(reqCount + "|" + totalBytes));
        }
    }

    private void runQuery1(String runId) throws Exception {
        System.out.println("   -> Executing MR Job: Query 1 (Daily Traffic)...");
        Path input = new Path(hdfsBaseDir + runId + "/input/");
        Path output = new Path(hdfsBaseDir + runId + "/q1_out/");
        
        Job job = Job.getInstance(new Configuration(), "Q1_DailyTraffic");
        job.setJarByClass(MapReducePipeline.class);
        job.setMapperClass(Q1Mapper.class);
        job.setReducerClass(Q1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);

        // Load to MariaDB
        loadQ1ToDB(output, runId);
    }

    private void loadQ1ToDB(Path outputDir, String runId) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        String sql = "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (FileStatus status : fs.listStatus(outputDir)) {
                if (!status.getPath().getName().startsWith("part-r-")) continue;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] kv = line.split("\t");
                        String[] keys = kv[0].split("\\|");
                        String[] vals = kv[1].split("\\|");
                        ps.setString(1, runId);
                        ps.setString(2, keys[0]);
                        ps.setInt(3, Integer.parseInt(keys[1]));
                        ps.setLong(4, Long.parseLong(vals[0]));
                        ps.setLong(5, Long.parseLong(vals[1]));
                        ps.addBatch();
                    }
                }
            }
            ps.executeBatch();
        }
    }

    // ==========================================
    // QUERY 2: TOP RESOURCES (MapReduce)
    // ==========================================
    public static class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 6) {
                // Key: resourcePath, Value: 1|bytes|host
                context.write(new Text(parts[3]), new Text("1|" + parts[5] + "|" + parts[0]));
            }
        }
    }

    public static class Q2Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long reqCount = 0;
            long totalBytes = 0;
            Set<String> distinctHosts = new HashSet<>();
            for (Text val : values) {
                String[] p = val.toString().split("\\|");
                reqCount += Long.parseLong(p[0]);
                totalBytes += Long.parseLong(p[1]);
                if (p.length > 2) distinctHosts.add(p[2]);
            }
            context.write(key, new Text(reqCount + "|" + totalBytes + "|" + distinctHosts.size()));
        }
    }

    private void runQuery2(String runId) throws Exception {
        System.out.println("   -> Executing MR Job: Query 2 (Top Resources)...");
        Path input = new Path(hdfsBaseDir + runId + "/input/");
        Path output = new Path(hdfsBaseDir + runId + "/q2_out/");
        
        Job job = Job.getInstance(new Configuration(), "Q2_TopResources");
        job.setJarByClass(MapReducePipeline.class);
        job.setMapperClass(Q2Mapper.class);
        job.setReducerClass(Q2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);

        loadQ2ToDB(output, runId);
    }

    private void loadQ2ToDB(Path outputDir, String runId) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        List<String[]> results = new ArrayList<>();
        
        for (FileStatus status : fs.listStatus(outputDir)) {
            if (!status.getPath().getName().startsWith("part-r-")) continue;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                String line;
                while ((line = br.readLine()) != null) {
                    results.add(line.split("\t"));
                }
            }
        }
        
        // Final Sort to get Top 20 per requirement
        results.sort((a, b) -> Long.compare(Long.parseLong(b[1].split("\\|")[0]), Long.parseLong(a[1].split("\\|")[0])));
        
        String sql = "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (int i = 0; i < Math.min(20, results.size()); i++) {
                String[] kv = results.get(i);
                String[] vals = kv[1].split("\\|");
                ps.setString(1, runId);
                ps.setString(2, kv[0]);
                ps.setLong(3, Long.parseLong(vals[0]));
                ps.setLong(4, Long.parseLong(vals[1]));
                ps.setLong(5, Long.parseLong(vals[2]));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // ==========================================
    // QUERY 3: HOURLY ERRORS (MapReduce)
    // ==========================================
    public static class Q3Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 6) {
                int status = Integer.parseInt(parts[4]);
                int isError = (status >= 400 && status <= 599) ? 1 : 0;
                String hostStr = isError == 1 ? parts[0] : "NONE";
                // Key: logDate|logHour, Value: isError|1|errorHost
                context.write(new Text(parts[1] + "|" + parts[2]), new Text(isError + "|1|" + hostStr));
            }
        }
    }

    public static class Q3Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long errCount = 0;
            long totCount = 0;
            Set<String> distinctHosts = new HashSet<>();
            for (Text val : values) {
                String[] p = val.toString().split("\\|");
                errCount += Long.parseLong(p[0]);
                totCount += Long.parseLong(p[1]);
                if (p.length > 2 && !p[2].equals("NONE")) distinctHosts.add(p[2]);
            }
            float errorRate = totCount == 0 ? 0 : (float) errCount / totCount;
            context.write(key, new Text(errCount + "|" + totCount + "|" + errorRate + "|" + distinctHosts.size()));
        }
    }

    private void runQuery3(String runId) throws Exception {
        System.out.println("   -> Executing MR Job: Query 3 (Hourly Errors)...");
        Path input = new Path(hdfsBaseDir + runId + "/input/");
        Path output = new Path(hdfsBaseDir + runId + "/q3_out/");
        
        Job job = Job.getInstance(new Configuration(), "Q3_HourlyErrors");
        job.setJarByClass(MapReducePipeline.class);
        job.setMapperClass(Q3Mapper.class);
        job.setReducerClass(Q3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);

        loadQ3ToDB(output, runId);
    }

    private void loadQ3ToDB(Path outputDir, String runId) throws Exception {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        String sql = "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = mysqlConn.prepareStatement(sql)) {
            for (FileStatus status : fs.listStatus(outputDir)) {
                if (!status.getPath().getName().startsWith("part-r-")) continue;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] kv = line.split("\t");
                        String[] keys = kv[0].split("\\|");
                        String[] vals = kv[1].split("\\|");
                        ps.setString(1, runId);
                        ps.setString(2, keys[0]);
                        ps.setString(3, keys[1]);
                        ps.setLong(4, Long.parseLong(vals[0]));
                        ps.setLong(5, Long.parseLong(vals[1]));
                        ps.setFloat(6, Float.parseFloat(vals[2]));
                        ps.setLong(7, Long.parseLong(vals[3]));
                        ps.addBatch();
                    }
                }
            }
            ps.executeBatch();
        }
    }
}