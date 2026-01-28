package io.confluent.ps.connect.metrics;

public interface TaskMetricsMXBean {
    long getQueueSize();
    boolean isRunning();
}
