package io.confluent.ps.connect.metrics;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

public class FetcherMetrics implements FetcherMetricsMXBean {
    private AtomicLong eventCount = new AtomicLong(0);
    private AtomicLong apiCallCount = new AtomicLong(0);
    private AtomicLong errorCount = new AtomicLong(0);
    private String lastError = "";
    private String lastErrorTimestamp = "";

    public FetcherMetrics() {
    }

    @Override
    public long getEvent_Count() {
        return eventCount.get();
    }

    public void incrementEvent_Count(long count) {
        eventCount.addAndGet(count);
    }

    @Override
    public long getApiCall_Count() {
        return apiCallCount.get();
    }

    public void incrementApiCall_Count(long count) {
        apiCallCount.addAndGet(count);
    }

    @Override
    public long getError_Count() {
        return errorCount.get();
    }

    public void incrementError_Count(long count) {
        errorCount.addAndGet(count);
    }

    @Override
    public String getLast_Error() {
        return lastError;
    }

    public void setLast_Error(String lastError) {
        this.lastError = lastError;
        this.lastErrorTimestamp = ZonedDateTime.now().toString();
    }

    @Override
    public String getLast_Error_Timestamp() {
        return lastErrorTimestamp;
    }

    public void setLast_Error_Timestamp(String lastErrorTimestamp) {
        this.lastErrorTimestamp = lastErrorTimestamp;
    }

}
