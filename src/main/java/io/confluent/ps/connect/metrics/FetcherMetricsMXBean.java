package io.confluent.ps.connect.metrics;

public interface FetcherMetricsMXBean {
    long getEvent_Count();
    long getApiCall_Count();
    long getError_Count();
    String getLast_Error();
    String getLast_Error_Timestamp();
}
