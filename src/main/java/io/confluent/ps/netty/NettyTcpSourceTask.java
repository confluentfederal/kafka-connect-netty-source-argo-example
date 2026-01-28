package io.confluent.ps.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyTcpSourceTask extends NettySourceTask {

    protected Class<? extends ChannelInitializer<? extends Channel>> getDefaultChannelInitializerClass() {
        return BytesTcpChannelInitializer.class;
    }

    @Override
    protected Channel createWorkerChannel(InetAddress bindAddress, Integer port, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup, ChannelInitializer<? extends Channel> channelInitializer) {
        InetSocketAddress bind = new InetSocketAddress(bindAddress, port);

        ServerBootstrap bootstrap = new ServerBootstrap();
        if (channelInitializer instanceof BytesTcpChannelInitializer) {
            ((BytesTcpChannelInitializer)channelInitializer).setMessageQueue(this.deque);
        }
        bootstrap
            .group(workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(channelInitializer)
            .option(ChannelOption.AUTO_CLOSE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true);

        try {
            Channel channel = bootstrap.bind(bind).sync().channel();
            return channel;
        } catch (Exception ex) {
            log.error("An error occurred binding the server", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected String getTaskMetricsType() {
        return "TcpTaskMetrics";
    }

}
