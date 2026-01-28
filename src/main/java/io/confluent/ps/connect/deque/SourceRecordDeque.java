package io.confluent.ps.connect.deque;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * A thread-safe deque for SourceRecords with batching support.
 */
public class SourceRecordDeque {
    private final LinkedBlockingDeque<SourceRecord> deque;
    private final int batchSize;
    private final long emptyWaitMs;
    private final long maximumCapacityTimeoutMs;
    private final long maximumCapacityWaitMs;

    public SourceRecordDeque(int maximumCapacity, int batchSize, long emptyWaitMs, 
                             long maximumCapacityTimeoutMs, long maximumCapacityWaitMs) {
        this.deque = new LinkedBlockingDeque<>(maximumCapacity);
        this.batchSize = batchSize;
        this.emptyWaitMs = emptyWaitMs;
        this.maximumCapacityTimeoutMs = maximumCapacityTimeoutMs;
        this.maximumCapacityWaitMs = maximumCapacityWaitMs;
    }

    /**
     * Add a record to the deque, blocking if at capacity.
     */
    public void add(SourceRecord record) throws InterruptedException {
        if (!deque.offer(record, maximumCapacityTimeoutMs, TimeUnit.MILLISECONDS)) {
            // If we couldn't add after timeout, try with longer wait
            deque.offer(record, maximumCapacityWaitMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Get a batch of records from the deque.
     */
    public List<SourceRecord> getBatch() throws InterruptedException {
        List<SourceRecord> batch = new ArrayList<>(batchSize);
        
        // Wait for at least one record
        SourceRecord first = deque.poll(emptyWaitMs, TimeUnit.MILLISECONDS);
        if (first != null) {
            batch.add(first);
            // Drain up to batchSize - 1 more records without blocking
            deque.drainTo(batch, batchSize - 1);
        }
        
        return batch;
    }

    /**
     * Get the current size of the deque.
     */
    public int size() {
        return deque.size();
    }

    /**
     * Check if the deque is empty.
     */
    public boolean isEmpty() {
        return deque.isEmpty();
    }
}
