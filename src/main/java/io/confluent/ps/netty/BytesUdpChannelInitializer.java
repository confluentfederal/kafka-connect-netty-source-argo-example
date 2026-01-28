package io.confluent.ps.netty;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.confluent.ps.connect.metrics.FetcherMetrics;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BytesUdpChannelInitializer extends NettySourceChannelInitializer<NioDatagramChannel> {

    private static FetcherMetrics fetcherMetrics;
    private ObjectName objectName;

    @Override
    protected void initChannel(NioDatagramChannel channel) throws Exception {
        log.debug("Initializing UDP channel");

        // Create the metrics instance
        if (null == BytesUdpChannelInitializer.fetcherMetrics) {
            try {
                BytesUdpChannelInitializer.fetcherMetrics = new FetcherMetrics();
                int port = this.config.getInt(NettySourceConnectorConfig.LISTEN_PORT_CONF);

                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                objectName = new ObjectName(
                    String.format(
                        "confluentps.connect.source.netty:type=UdpFetcherMetrics,protocol={},port={},channel={}",
                        "udp",
                        port,
                        channel.id().asShortText()
                    )
                );
                // See if the mbean is already registered
                if (mbs.isRegistered(objectName)) {
                    log.warn("Fetcher MBean with name {} is already registered. This may be due to a start/stop operation. The existing MBean will be unregistered.", objectName.toString());
                    mbs.unregisterMBean(objectName);
                }
                mbs.registerMBean(BytesUdpChannelInitializer.fetcherMetrics, objectName);
            } catch (Exception e) {
                log.error("Error registering FetcherMetrics MBean: " + e.getMessage(), e);
            }
        }

        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("framer", new SinglePacketHandler());
        SourceRecordHandler handler = null;
        // Note: value.converter is defined in org.apache.kafka.connect.runtime.WorkerConfig
        String valueConverter = this.config.getString("value.converter");
        if (valueConverter != null && valueConverter.equalsIgnoreCase("org.apache.kafka.connect.storage.StringConverter")) {
            log.debug("Detected org.apache.kafka.connect.storage.StringConverter for value.converter, using StringRecordHandler");
            handler = new StringRecordHandler();
        } else {
            log.debug("Using StructuredRecordHandler for non-StringConverter value.converter");
            handler = new StructuredRecordHandler();
        }
        handler.setTopic(topic);
        handler.setRecordQueue(messageQueue);
        handler.setFetcherMetrics(BytesUdpChannelInitializer.fetcherMetrics);
        pipeline.addLast("recordHandler", handler);
    }

    @Override
    public void close() throws IOException {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(objectName);
        } catch (Exception e) {
            log.warn("Failed to unregister MBean", e);
        }
    }
    
}
