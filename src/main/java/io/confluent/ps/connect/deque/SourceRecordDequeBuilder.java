package io.confluent.ps.connect.deque;

/**
 * Builder for SourceRecordDeque.
 */
public class SourceRecordDequeBuilder {
    private int maximumCapacity = 10000;
    private int batchSize = 1000;
    private long emptyWaitMs = 100;
    private long maximumCapacityTimeoutMs = 1000;
    private long maximumCapacityWaitMs = 5000;

    private SourceRecordDequeBuilder() {}

    public static SourceRecordDequeBuilder of() {
        return new SourceRecordDequeBuilder();
    }

    public SourceRecordDequeBuilder maximumCapacity(int maximumCapacity) {
        this.maximumCapacity = maximumCapacity;
        return this;
    }

    public SourceRecordDequeBuilder batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public SourceRecordDequeBuilder emptyWaitMs(long emptyWaitMs) {
        this.emptyWaitMs = emptyWaitMs;
        return this;
    }

    public SourceRecordDequeBuilder maximumCapacityTimeoutMs(long maximumCapacityTimeoutMs) {
        this.maximumCapacityTimeoutMs = maximumCapacityTimeoutMs;
        return this;
    }

    public SourceRecordDequeBuilder maximumCapacityWaitMs(long maximumCapacityWaitMs) {
        this.maximumCapacityWaitMs = maximumCapacityWaitMs;
        return this;
    }

    public SourceRecordDeque build() {
        return new SourceRecordDeque(
            maximumCapacity, 
            batchSize, 
            emptyWaitMs, 
            maximumCapacityTimeoutMs, 
            maximumCapacityWaitMs
        );
    }
}
