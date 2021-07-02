package com.flink.demo.cases.case25;

import org.apache.flink.runtime.metrics.MetricNames;

public class MetricsNames {

    public static final String METRIC_GROUP = "metric_train";

    public static final String INPUT_RECORDS = "input_records";
    public static final String OUTPUT_RECORDS = "output_records";

    public static final String PROCESS_TIME = "process_time";
    public static final String TOOK_TIME = "took_time";

    public static final String PROCESSED_RECORD_RATE = "processedRecord" + MetricNames.SUFFIX_RATE;

}
