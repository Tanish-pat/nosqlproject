package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import java.util.List;
import java.util.Map;

public interface Pipeline {
    void loadBatch(List<NasaLogRecord> batch, String runId, int batchId);
    Map<String, Long> executeQueries(String runId);
    String getPipelineName();
}