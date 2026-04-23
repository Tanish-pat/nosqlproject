package com.nosql.pipelines;

import com.nosql.parser.NasaLogRecord;
import java.util.List;

public interface Pipeline {
    // Phase 1: Load the raw parsed data into the NoSQL backend
    void loadBatch(List<NasaLogRecord> batch, String runId, int batchId);
    
    // Phase 2: Execute Q1, Q2, Q3 using the NoSQL engine and save to MariaDB
    void executeQueries(String runId);

    String getPipelineName();
}