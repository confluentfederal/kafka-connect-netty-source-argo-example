package io.confluent.ps.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyUdpSourceTask extends NettySourceTask  {

    protected Class<? extends ChannelInitializer<? extends Channel>> getDefaultChannelInitializerClass() {
        return BytesUdpChannelInitializer.class;
    }

    @Override
    protected Channel createWorkerChannel(InetAddress bindAddress, Integer port, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup, ChannelInitializer<? extends Channel> channelInitializer) {
        InetSocketAddress bind = new InetSocketAddress(bindAddress, port);

        Bootstrap bootstrap = new Bootstrap();
        if (channelInitializer instanceof BytesUdpChannelInitializer) {
            ((BytesUdpChannelInitializer)channelInitializer).setMessageQueue(this.deque);
        }
        bootstrap
            .group(workerGroup)
            .channel(NioDatagramChannel.class)
            .handler(channelInitializer)
            // .option(ChannelOption.SO_RCVBUF, 2048)
            // .option(ChannelOption.AUTO_CLOSE, true)
            .option(ChannelOption.SO_BROADCAST, true);

        try {
            Channel channel = bootstrap.bind(bind).sync().channel();
            log.info("Returning channel");
            return channel;
        } catch (Exception ex) {
            log.error("An error occurred binding the server", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected String getTaskMetricsType() {
        return "UdpTaskMetrics";
    }

}
