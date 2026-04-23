package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import java.util.List;

public interface Pipeline {
    void loadBatch(List<NasaLogRecord> batch, String runId, int batchId);
    void executeQueries(String runId);
    String getPipelineName();
}