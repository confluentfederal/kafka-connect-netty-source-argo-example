package io.confluent.ps.connect.deque;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * A thread-safe deque for SourceRecords with batching support.
 * Extends LinkedBlockingDeque to implement Deque interface.
 */
public class SourceRecordDeque extends LinkedBlockingDeque<SourceRecord> {
    private final int batchSize;
    private final long emptyWaitMs;
    private final long maximumCapacityTimeoutMs;
    private final long maximumCapacityWaitMs;

    public SourceRecordDeque(int maximumCapacity, int batchSize, long emptyWaitMs, 
                             long maximumCapacityTimeoutMs, long maximumCapacityWaitMs) {
        super(maximumCapacity);
        this.batchSize = batchSize;
        this.emptyWaitMs = emptyWaitMs;
        this.maximumCapacityTimeoutMs = maximumCapacityTimeoutMs;
        this.maximumCapacityWaitMs = maximumCapacityWaitMs;
    }

    /**
     * Add a record to the deque, blocking if at capacity.
     */
    public void addRecord(SourceRecord record) throws InterruptedException {
        if (!offer(record, maximumCapacityTimeoutMs, TimeUnit.MILLISECONDS)) {
            // If we couldn't add after timeout, try with longer wait
            offer(record, maximumCapacityWaitMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Get a batch of records from the deque.
     */
    public List<SourceRecord> getBatch() throws InterruptedException {
        List<SourceRecord> batch = new ArrayList<>(batchSize);
        
        // Wait for at least one record
        SourceRecord first = poll(emptyWaitMs, TimeUnit.MILLISECONDS);
        if (first != null) {
            batch.add(first);
            // Drain up to batchSize - 1 more records without blocking
            drainTo(batch, batchSize - 1);
        }
        
        return batch;
    }
}
