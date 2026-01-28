package io.confluent.ps.connect;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordDeque;

import io.confluent.ps.connect.metrics.FetcherMetrics;
import io.confluent.ps.connect.metrics.TaskMetrics;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSourceTaskWithMetrics extends SourceTask {

    private SourceRecordDeque deque;
    private AtomicBoolean running;
    protected RuntimeException raisedException;

    protected TaskMetrics taskMetrics;

    protected ExecutorService executorService;
    protected Thread taskMetricsMonitor;

    /**
     * Indicate whether the task is running.
     *
     * @return
     */
    public boolean isRunning() {
        return null != this.running && this.running.get();
    }

    /**
     * Get the deque used to buffer SourceRecords.
     *
     * @return
     */
    public SourceRecordDeque getDeque() {
        return this.deque;
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // If there is a raised exception and we are running then stop running
        if (null != this.raisedException && (null != this.running && this.running.get())) {
            // Write the exception to the log and stop everything
            log.error("Raised exception, stopping task", this.raisedException);
            this.cleanup();
        }

        // If the deque is empty and there is a raised exception, throw it
        if (deque.isEmpty() && null != this.raisedException) {
            throw this.raisedException;
        }

        return this.deque.getBatch();
    }

    /**
     * Cleanup any timers that exist
     */
    protected void cleanup() {
        log.info("Cleaning up task");
        this.preCleanup();
        if (null != this.running) {
            this.running.set(false);
        }
        if (null != this.executorService) {
            this.executorService.shutdown();
            try {
                if (!this.executorService.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    log.warn("Executor service did not terminate in the specified time, forcing a shutdown.");
                    this.executorService.shutdownNow();
                } else {
                    log.info("Executor service terminated successfully.");
                }
            } catch (InterruptedException e) {
                log.error("Executor service was interrupted during shutdown.", e);
                Thread.currentThread().interrupt(); // Restore the interrupted status
            }
        }
        this.postCleanup();
    }

    @Override
    public void start(Map<String, String> props) {
        log.debug("\n{}",
                props.entrySet().stream().map(entry -> entry.getKey() + " = " + entry.getValue())
                        .collect(Collectors.joining("\n")));

        this.cleanup();

        if (null != this.deque) {
            log.warn("The deque has already been initialized. This may be due to a start/stop operation.");
        } else {
            this.deque = createSourceRecordDeque(props);
        }

        this.preStart(props);

        // Inform everything is ready to run
        this.running = new AtomicBoolean(true);

        // Register the JMX MBean for the fetcher
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        int threadCount = getThreadCount(props);
        this.executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            try {
                FetcherMetrics metrics = new FetcherMetrics();
                ObjectName objectName = new ObjectName(createFetcherObjectName(props, i));
                mbs.registerMBean(metrics, objectName);

                // Note: this starts the fetcher threads immediately
                AbstractFetcherWithMetrics fetcher = createFetcherInstance(props, i);
                fetcher.setMetrics(metrics);

                this.executorService.execute(fetcher);
            } catch (Exception ex) {
                log.error("An error occurred while starting a fetcher thread", ex);
            }
        }

        if (null == this.taskMetrics) {
            try {
                this.taskMetrics = new TaskMetrics();
                ObjectName objectName = new ObjectName(createTaskObjectName(props));
                mbs.registerMBean(this.taskMetrics, objectName);

                // Start the metrics thread
                this.taskMetricsMonitor = new Thread(() -> {
                    while (true) {
                        try {
                            Thread.sleep(30000);
                            this.taskMetrics.setQueueSize(this.deque.size());
                            this.taskMetrics.setRunning(this.running.get());
                        } catch (InterruptedException e) {
                            log.error("Task metrics thread interrupted", e);
                            Thread.currentThread().interrupt(); // Restore the interrupted status
                        }
                    }
                });
                this.taskMetricsMonitor.setDaemon(true);
                this.taskMetricsMonitor.start();
            } catch (Exception ex) {
                log.error("An error occurred while registering the task metrics MBean", ex);
            }
        } else {
            log.warn("The task metrics have already been initialized. This may be due to a start/stop operation.");
        }

        this.postStart(props);
    }

    @Override
    public void stop() {
        log.info("Stopping task");
        if (null == this.running) {
            this.running = new AtomicBoolean(false);
        } else {
            this.running.set(false);
        }
        this.cleanup();
    }

    /**
     * Hook called before starting the fetchers and metrics.
     *
     * @param props
     */
    public abstract void preStart(Map<String, String> props);

    /**
     * Hook called after starting the fetchers and metrics.
     *
     * @param props
     */
    public abstract void postStart(Map<String, String> props);

    /**
     * Hook called before cleaning up the fetchers and metrics.
     */
    public abstract void preCleanup();

    /**
     * Hook called after cleaning up the fetchers and metrics.
     */
    public abstract void postCleanup();

    /**
     * Get the number of fetcher threads to create.
     *
     * @param props
     * @return
     */
    public abstract int getThreadCount(Map<String, String> props);

    /**
     * Create the JMX ObjectName for the task MBean. For example,
     * "confluentps.connect.source.menlo:type=MenloTaskMetrics,tenant=" + this.tenantName + ",logType=" + this.logType
     *
     * @param props
     * @return
     */
    public abstract String createTaskObjectName(Map<String, String> props);

    /**
     * Create the JMX ObjectName for the fetcher MBean. For example,
     * "confluentps.connect.source.menlo:type=MenloFetcherMetrics,tenant=" + this.tenantName + ",logType=" + this.logType + ",fetcher=" + i
     *
     * @param fetcherIndex
     * @return
     */
    public abstract String createFetcherObjectName(Map<String, String> props, int fetcherIndex);

    /**
     * Create a fetcher instance used to read data and place it into the deque.
     *
     * @param props
     * @param fetcherIndex
     * @return
     * @throws RuntimeException if there is an error creating the fetcher
     */
    public abstract AbstractFetcherWithMetrics createFetcherInstance(Map<String, String> props, int fetcherIndex) throws RuntimeException;

    /**
     * Create the dequeue instance used to buffer SourceRecords between fetchers and the poll() method. For example,
     * return SourceRecordDequeBuilder.of()
     *     .maximumCapacity(queueMaximumCapacity)
     *     .batchSize(queueBatchSize)
     *     .emptyWaitMs(queueEmptyWaitMs)
     *     .maximumCapacityTimeoutMs(queueMaximumCapacityTimeoutMs)
     *     .maximumCapacityWaitMs(queueMaximumCapacityWaitMs)
     *     .build();
     *
     * @param props
     * @return
     */
    public abstract SourceRecordDeque createSourceRecordDeque(Map<String, String> props);

}
