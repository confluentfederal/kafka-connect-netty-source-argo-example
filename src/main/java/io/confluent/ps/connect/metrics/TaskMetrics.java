package io.confluent.ps.connect.metrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TaskMetrics implements TaskMetricsMXBean {
    private AtomicLong queueSize = new AtomicLong(0);
    private AtomicBoolean running = new AtomicBoolean(false);

    public TaskMetrics() {
    }

    @Override
    public long getQueueSize() {
        return this.queueSize.get();
    }

    public void setQueueSize(long size) {
        this.queueSize.set(size);
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

    public void setRunning(boolean running) {
        this.running.set(running);
    }

}
