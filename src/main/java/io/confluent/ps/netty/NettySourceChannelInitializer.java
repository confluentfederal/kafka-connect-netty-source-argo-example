package io.confluent.ps.netty;

import java.io.Closeable;
import java.util.Deque;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.source.SourceRecord;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public abstract class NettySourceChannelInitializer<T extends Channel> extends ChannelInitializer<T> implements Configurable, Closeable {

    protected NettySourceConnectorConfig config;
    protected Deque<SourceRecord> messageQueue;
    protected String topic;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new NettySourceConnectorConfig((Map<String, String>)configs);
        this.topic = config.getString(NettySourceConnectorConfig.KAFKA_TOPIC_CONF);
    }

    public void setMessageQueue(Deque<SourceRecord> messageQueue) {
        this.messageQueue = messageQueue;
    }
}
