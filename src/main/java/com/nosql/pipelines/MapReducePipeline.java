package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.sql.*;
import java.util.*;

public class MapReducePipeline implements Pipeline {

    private final Connection pgConn;
    private final String baseDir = "/tmp/nasa_logs_mr/";

    public MapReducePipeline(Connection pgConn) {
        this.pgConn = pgConn;
    }

    @Override
    public String getPipelineName() {
        return "MapReduce";
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
            timings.put("Q1_DAILY_TRAFFIC",
                runJob(runId, "Q1", Q1Mapper.class, Q1Reducer.class, "q1_out",
                    (ps, k, v) -> {
                        ps.setString(1, runId);
                        ps.setString(2, k.date);
                        ps.setInt(3, k.status);
                        ps.setLong(4, v.count);
                        ps.setLong(5, v.bytes);
                    },
                    "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES (?,?,?,?,?)"
                )
            );

            timings.put("Q2_TOP_RESOURCES",
                runJob(runId, "Q2", Q2Mapper.class, Q2Reducer.class, "q2_out",
                    (ps, k, v) -> {
                        ps.setString(1, runId);
                        ps.setString(2, k.resource);
                        ps.setLong(3, v.count);
                        ps.setLong(4, v.bytes);
                        ps.setLong(5, v.hosts);
                    },
                    "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES (?,?,?,?,?)"
                )
            );

            timings.put("Q3_HOURLY_ERRORS",
                runJob(runId, "Q3", Q3Mapper.class, Q3Reducer.class, "q3_out",
                    (ps, k, v) -> {
                        ps.setString(1, runId);
                        ps.setString(2, k.date);
                        ps.setString(3, k.hour);
                        ps.setLong(4, v.errors);
                        ps.setLong(5, v.total);
                        ps.setDouble(6, v.total == 0 ? 0.0 : (double) v.errors / v.total);
                        ps.setLong(7, v.hosts);
                    },
                    "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES (?,?,?,?,?,?,?)"
                )
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return timings;
    }

    private long runJob(
        String runId,
        String name,
        Class<? extends Mapper<?, ?, ?, ?>> m,
        Class<? extends Reducer<?, ?, ?, ?>> r,
        String outDir,
        RowMapper mapper,
        String sql
    ) throws Exception {

        long start = System.currentTimeMillis();

        Path inPath  = new Path(baseDir + runId + "/input/");
        Path outPath = new Path(baseDir + runId + "/" + outDir);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        if (fs.exists(outPath)) fs.delete(outPath, true);

        Job job = Job.getInstance(conf, name);
        job.setJarByClass(MapReducePipeline.class);

        job.setMapperClass(m);
        job.setReducerClass(r);

        job.setMapOutputKeyClass(KeyWritable.class);
        job.setMapOutputValueClass(ValueWritable.class);
        job.setOutputKeyClass(KeyWritable.class);
        job.setOutputValueClass(ValueWritable.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("MapReduce job " + name + " failed");
        }

        try (PreparedStatement ps = pgConn.prepareStatement(sql)) {

            for (FileStatus f : fs.listStatus(outPath)) {
                if (!f.getPath().getName().startsWith("part")) continue;

                try (BufferedReader br =
                         new BufferedReader(new InputStreamReader(fs.open(f.getPath())))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t", 2);
                        if (parts.length != 2) continue;

                        KeyWritable  k = KeyWritable.fromString(parts[0]);
                        ValueWritable v = ValueWritable.fromString(parts[1]);

                        mapper.map(ps, k, v);
                        ps.addBatch();
                    }
                }
            }

            ps.executeBatch();
        }

        return System.currentTimeMillis() - start;
    }

    public static class KeyWritable implements WritableComparable<KeyWritable> {

        String date     = "";
        String hour     = "";
        String resource = "";
        int    status   = 0;

        public KeyWritable() {}

        static KeyWritable fromString(String s) {
            String[] p = s.split("\\|", -1);
            KeyWritable k = new KeyWritable();
            if (p.length > 0) k.date     = p[0];
            if (p.length > 1) k.hour     = p[1];
            if (p.length > 2) k.resource = p[2];
            if (p.length > 3) { try { k.status = Integer.parseInt(p[3]); } catch (Exception ignored) {} }
            return k;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(date);
            out.writeUTF(hour);
            out.writeUTF(resource);
            out.writeInt(status);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            date     = in.readUTF();
            hour     = in.readUTF();
            resource = in.readUTF();
            status   = in.readInt();
        }

        @Override
        public int compareTo(KeyWritable o) {
            int c = date.compareTo(o.date);
            if (c != 0) return c;
            c = hour.compareTo(o.hour);
            if (c != 0) return c;
            c = resource.compareTo(o.resource);
            if (c != 0) return c;
            return Integer.compare(status, o.status);
        }

        @Override
        public String toString() {
            return date + "|" + hour + "|" + resource + "|" + status;
        }
    }

    public static class ValueWritable implements Writable {

        long   count  = 0;
        long   bytes  = 0;
        long   hosts  = 0;
        long   total  = 0;
        long   errors = 0;
        String hostList = "";

        public ValueWritable() {}

        static ValueWritable fromString(String s) {
            String[] p = s.split("\\|", -1);
            ValueWritable v = new ValueWritable();
            if (p.length > 0) { try { v.count  = Long.parseLong(p[0]); } catch (Exception ignored) {} }
            if (p.length > 1) { try { v.bytes  = Long.parseLong(p[1]); } catch (Exception ignored) {} }
            if (p.length > 2) { try { v.hosts  = Long.parseLong(p[2]); } catch (Exception ignored) {} }
            if (p.length > 3) { try { v.total  = Long.parseLong(p[3]); } catch (Exception ignored) {} }
            if (p.length > 4) { try { v.errors = Long.parseLong(p[4]); } catch (Exception ignored) {} }
            return v;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(count);
            out.writeLong(bytes);
            out.writeLong(hosts);
            out.writeLong(total);
            out.writeLong(errors);
            out.writeUTF(hostList);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count    = in.readLong();
            bytes    = in.readLong();
            hosts    = in.readLong();
            total    = in.readLong();
            errors   = in.readLong();
            hostList = in.readUTF();
        }

        @Override
        public String toString() {
            return count + "|" + bytes + "|" + hosts + "|" + total + "|" + errors;
        }
    }

    @FunctionalInterface
    interface RowMapper {
        void map(PreparedStatement ps, KeyWritable k, ValueWritable v) throws SQLException;
    }

    public static class Q1Mapper extends Mapper<LongWritable, Text, KeyWritable, ValueWritable> {
        @Override
        public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] p = v.toString().split("\t", -1);
            if (p.length < 6) return;

            KeyWritable key = new KeyWritable();
            key.date   = p[1];
            key.status = parseInt(p[4]);

            ValueWritable val = new ValueWritable();
            val.count = 1;
            val.bytes = parseLong(p[5]);

            c.write(key, val);
        }
    }

    public static class Q1Reducer extends Reducer<KeyWritable, ValueWritable, KeyWritable, ValueWritable> {
        @Override
        public void reduce(KeyWritable k, Iterable<ValueWritable> vs, Context c)
                throws IOException, InterruptedException {
            ValueWritable out = new ValueWritable();
            for (ValueWritable v : vs) {
                out.count += v.count;
                out.bytes += v.bytes;
            }
            c.write(k, out);
        }
    }

    public static class Q2Mapper extends Mapper<LongWritable, Text, KeyWritable, ValueWritable> {
        @Override
        public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] p = v.toString().split("\t", -1);
            if (p.length < 6) return;

            KeyWritable key = new KeyWritable();
            key.resource = p[3];

            ValueWritable val = new ValueWritable();
            val.count    = 1;
            val.bytes    = parseLong(p[5]);
            val.hostList = p[0]; // emit host for deduplication in reducer

            c.write(key, val);
        }
    }

    public static class Q2Reducer extends Reducer<KeyWritable, ValueWritable, KeyWritable, ValueWritable> {
        @Override
        public void reduce(KeyWritable k, Iterable<ValueWritable> vs, Context c)
                throws IOException, InterruptedException {
            ValueWritable out = new ValueWritable();
            Set<String> hostSet = new HashSet<>();

            for (ValueWritable v : vs) {
                out.count += v.count;
                out.bytes += v.bytes;
                if (!v.hostList.isEmpty()) hostSet.add(v.hostList);
            }

            out.hosts = hostSet.size(); // correct distinct host count
            c.write(k, out);
        }
    }

    public static class Q3Mapper extends Mapper<LongWritable, Text, KeyWritable, ValueWritable> {
        @Override
        public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String[] p = v.toString().split("\t", -1);
            if (p.length < 6) return;

            int status = parseInt(p[4]);
            boolean isError = status >= 400 && status <= 599;

            KeyWritable key = new KeyWritable();
            key.date = p[1];
            key.hour = p[2];

            ValueWritable val = new ValueWritable();
            val.total    = 1;
            val.errors   = isError ? 1 : 0;
            val.hostList = isError ? p[0] : ""; // only carry host for error records

            c.write(key, val);
        }
    }

    public static class Q3Reducer extends Reducer<KeyWritable, ValueWritable, KeyWritable, ValueWritable> {
        @Override
        public void reduce(KeyWritable k, Iterable<ValueWritable> vs, Context c)
                throws IOException, InterruptedException {
            ValueWritable out = new ValueWritable();
            Set<String> errorHostSet = new HashSet<>();

            for (ValueWritable v : vs) {
                out.total  += v.total;
                out.errors += v.errors;
                if (!v.hostList.isEmpty()) errorHostSet.add(v.hostList);
            }

            out.hosts = errorHostSet.size();
            c.write(k, out);
        }
    }

    private String safe(String s) {
        return s == null ? "-" : s;
    }

    private static int parseInt(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return 0; }
    }

    private static long parseLong(String s) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return 0; }
    }
}