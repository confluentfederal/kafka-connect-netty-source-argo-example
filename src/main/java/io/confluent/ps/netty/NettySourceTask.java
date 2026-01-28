/**
 * Copyright  Vitalii Rudenskyi (vrudenskyi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.ps.netty;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.ps.connect.deque.SourceRecordDeque;
import io.confluent.ps.connect.deque.SourceRecordDequeBuilder;
import io.confluent.ps.connect.metrics.TaskMetrics;
import io.confluent.ps.utils.VersionUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

public abstract class NettySourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(NettySourceTask.class);

  protected String taskName = "NettySourceTask";
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  private Channel workerChannel;
  private ChannelInitializer<? extends Channel> channelInitializer;

  protected NettySourceConnectorConfig connConfig;

  protected SourceRecordDeque deque;

  private AtomicBoolean running;

  // Cache of registered mbeans to unregister on stop
  protected List<ObjectName> registeredMBeanNames;

  private TaskMetrics taskMetrics;
  private Thread taskMetricsMonitor;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void start(Map<String, String> props) {
    try {
      connConfig = new NettySourceConnectorConfig(props);
      final Integer port = connConfig.getInt(NettySourceConnectorConfig.LISTEN_PORT_CONF);

      // configure task representation String
      this.taskName = this.getClass().getSimpleName() + " (bind: "
          + connConfig.getString(NettySourceConnectorConfig.BIND_ADDRESS_CONF) + " port:" + port + ")";

      final InetAddress bindAddress = InetAddress
          .getByName(connConfig.getString(NettySourceConnectorConfig.BIND_ADDRESS_CONF));

      int queueMaximumCapacity = connConfig.getInt(NettySourceConnectorConfig.QUEUE_CAPACITY_CONF);
      int queueBatchSize = connConfig.getInt(NettySourceConnectorConfig.QUEUE_BATCH_SIZE_CONF);
      int queueEmptyWaitMs = connConfig.getInt(NettySourceConnectorConfig.QUEUE_EMPTY_WAIT_MS_CONF);
      int queueMaximumCapacityTimeoutMs = connConfig.getInt(NettySourceConnectorConfig.QUEUE_CAPACITY_TIMEOUT_MS_CONF);
      int queueMaximumCapacityWaitMs = connConfig.getInt(NettySourceConnectorConfig.QUEUE_CAPACITY_WAIT_MS_CONF);

      this.deque = SourceRecordDequeBuilder.of()
          .maximumCapacity(queueMaximumCapacity)
          .batchSize(queueBatchSize)
          .emptyWaitMs(queueEmptyWaitMs)
          .maximumCapacityTimeoutMs(queueMaximumCapacityTimeoutMs)
          .maximumCapacityWaitMs(queueMaximumCapacityWaitMs)
          .build();

      Class<?> chInitClass = this.getDefaultChannelInitializerClass();

      channelInitializer = (ChannelInitializer<Channel>) Utils.newInstance(chInitClass);
      if (channelInitializer instanceof Configurable) {
        ((Configurable) channelInitializer).configure(props);
      }

      this.registerTaskMetrics(connConfig);

      this.bossGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
      this.workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

      this.workerChannel = this.createWorkerChannel(bindAddress, port, this.bossGroup,
          this.workerGroup, channelInitializer);
    } catch (Exception e) {
      throw new ConnectException("NettySourceTask failed to start for topic: ", e);
    }

    this.running = new AtomicBoolean(false);
    log.info("{} started.", this.taskName);

  }

  /**
   * Indicate whether the task is running.
   *
   * @return
   */
  public boolean isRunning() {
      return null != this.running && this.running.get();
  }

  /**
   * Creates address for the first available port
   * 
   * @param bindAddress
   * @param ports
   * @return
   */
  protected InetSocketAddress selectTcpSocketAddress(InetAddress bindAddress, List<Integer> ports) {
    log.debug("selecting TCP port for: {} from {}", bindAddress, ports);
    for (Integer port : ports) {
      try (ServerSocket ss = new ServerSocket(port, 0, bindAddress)) {
        log.debug("\tTCP Port Selected: {}", port);
        return new InetSocketAddress(bindAddress, port);
      } catch (Exception e) {
        log.debug("\tTCP Port In Use: {}", port);
      }

    }

    throw new ConnectException("[" + this.taskName + "]: all port are in use. ");

  }

  protected InetSocketAddress selectUdpSocketAddress(InetAddress bindAddress, List<Integer> ports) {
    log.debug("selecting UDP port for: {} from {}", bindAddress, ports);
    for (Integer port : ports) {
      try (DatagramSocket ds = new DatagramSocket(port, bindAddress)) {
        log.debug("\tUDP Port Selected: {}", port);
        return new InetSocketAddress(bindAddress, port);
      } catch (Exception e) {
        log.debug("\tUDP Port In Use: {}", port);
      }

    }

    throw new ConnectException("[" + this.taskName + "]: all port are in use. ");
  }

  protected abstract Class<? extends ChannelInitializer<? extends Channel>> getDefaultChannelInitializerClass();

  protected abstract Channel createWorkerChannel(InetAddress bindAddress, Integer port,
      EventLoopGroup bossGroup, EventLoopGroup workerGroup,
      ChannelInitializer<? extends Channel> pipelineFactory);

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (log.isTraceEnabled()) {
      if (this.deque.size() > 0) {
        log.trace("poll: deque size {}", this.deque.size());
      }
    }
    return this.deque.getBatch();
  }

  @Override
  public void stop() {
    log.info("Cleaning up task (interrupted={})", Thread.currentThread().isInterrupted());
    if (null != this.running) {
      this.running.set(false);
    }
    if (this.channelInitializer instanceof Closeable) {
      try {
        ((Closeable) (this.channelInitializer)).close();
      } catch (IOException e) {
        log.warn("Failed to close channel initializer", e);
      }
    }

    if (null != this.taskMetricsMonitor) {
      this.taskMetricsMonitor.interrupt();
      this.taskMetricsMonitor = null;
    }

    // Try to unregister the MBeans
    if (null != this.registeredMBeanNames && !this.registeredMBeanNames.isEmpty()) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        for (ObjectName objectName : this.registeredMBeanNames) {
            try {
                if (mbs.isRegistered(objectName)) {
                    mbs.unregisterMBean(objectName);
                }
            } catch (Exception e) {
                log.warn("Failed to unregister MBean with name {}", objectName, e);
            }
        }
        this.registeredMBeanNames.clear();
        this.registeredMBeanNames = null;
    }

    if (null != this.workerChannel) {
      this.workerChannel.close().awaitUninterruptibly(10000);
    }

    log.debug("Worker Channel shut down: {}", this.taskName);
    log.debug("Stopped {}", this.taskName);

    taskName = null;
  }

  private void registerTaskMetrics(NettySourceConnectorConfig config) {
    if (null == this.taskMetrics) {
      try {
        int port = config.getInt(NettySourceConnectorConfig.LISTEN_PORT_CONF);
        String protocol = config.getString(NettySourceConnectorConfig.PROTOCOL_CONF);

        // Register the JMX MBean for the fetcher
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        this.registeredMBeanNames = new ArrayList<>();

        this.taskMetrics = new TaskMetrics();

        ObjectName objectName = new ObjectName(String.format(
          "confluentps.connect.source.netty:type=%s,protocol=%s,port=%d",
          this.getTaskMetricsType(),
          protocol.toLowerCase(),
          port
        ));
        // See if the mbean is already registered
        if (mbs.isRegistered(objectName)) {
          log.warn(
              "TaskMetrics MBean with name {} is already registered. This may be due to a start/stop operation. The existing MBean will be unregistered.",
              objectName.toString());
          mbs.unregisterMBean(objectName);
        }
        mbs.registerMBean(this.taskMetrics, objectName);
        this.registeredMBeanNames.add(objectName);

        // Start the metrics thread
        this.taskMetricsMonitor = new Thread(() -> {
          while (this.isRunning()) {
            try {
              Thread.sleep(30000);
              this.taskMetrics.setQueueSize(this.deque.size());
              this.taskMetrics.setRunning(true);
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

  }

  protected abstract String getTaskMetricsType();

}
