package io.confluent.ps.netty;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.net.ssl.SSLEngine;

import org.apache.commons.lang3.StringUtils;

import io.confluent.ps.connect.metrics.FetcherMetrics;
import io.confluent.ps.utils.CertificateUtils;
import io.confluent.ps.utils.CertificateUtils.InvalidAliasException;
import io.confluent.ps.utils.CertificateUtils.InvalidPasswordException;
import io.confluent.ps.utils.CertificateUtils.KeyStoreAccessException;
import io.confluent.ps.utils.CertificateUtils.KeyStoreEmptyException;
import io.confluent.ps.utils.CertificateUtils.KeyStoreTypeException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BytesTcpChannelInitializer extends NettySourceChannelInitializer<Channel> {

    private boolean stripDelimiter = true;
    private boolean failFast = false;
    private ReadTimeoutHandler readTimeoutHandler;
    private List<ByteBuf> delimiters;

    private SslContext sslContext = null;
    private Iterable<? extends X509Certificate> certificateChain = null;
    private PrivateKey privateKey = null;

    private static FetcherMetrics fetcherMetrics;
    private ObjectName objectName;

    public BytesTcpChannelInitializer() {
        log.info("Instantiating BytesTcpChannelInitializer");
        this.delimiters = new ArrayList<>();
        Collections.addAll(this.delimiters, Delimiters.lineDelimiter());
        Collections.addAll(this.delimiters, Delimiters.nulDelimiter());
    }

    @Override
    protected void initChannel(Channel channel) {
        log.debug("Initializing TCP channel");

        ChannelPipeline pipeline = channel.pipeline();

        // Add other necessary handlers here, such as SSL/TLS handlers, logging
        // handlers, or your custom business logic handlers.
        String protocol = this.config.getString(NettySourceConnectorConfig.PROTOCOL_CONF);
        if ("TCPSSL".equalsIgnoreCase(protocol) || "HTTPS".equalsIgnoreCase(protocol)) {
            try {
                if (null == this.sslContext) {
                    String keystoreType = this.config.getString(NettySourceConnectorConfig.SSL_KEYSTORE_TYPE_CONF);
                    log.info("Initializing SSL Context with keystore type: {}", keystoreType);

                    if ("pem".equalsIgnoreCase(keystoreType)) {
                        String certfileLocation = this.config.getString(NettySourceConnectorConfig.SSL_CERTFILE_LOCATION_CONF);
                        String keyfileLocation = this.config.getString(NettySourceConnectorConfig.SSL_KEYFILE_LOCATION_CONF);
                        String keyfilePassword = this.config.getPassword(NettySourceConnectorConfig.SSL_KEYFILE_PASSWORD_CONF).value();
                        if (log.isDebugEnabled()) {
                            log.debug("PEM cert file location: {}", certfileLocation);
                            log.debug("PEM key file location: {}", keyfileLocation);
                            log.debug("PEM key file password provided: {}", StringUtils.isNotBlank(keyfilePassword));
                        }
                        if (null == keyfilePassword || StringUtils.isBlank(keyfilePassword)) {
                            log.info("Initializing SSL Context with PEM files without key password");
                            this.sslContext = SslContextBuilder.forServer(new File(certfileLocation), new File(keyfileLocation)).build();
                        } else {
                            log.info("Initializing SSL Context with PEM files with key password");
                            this.sslContext = SslContextBuilder.forServer(new File(certfileLocation), new File(keyfileLocation), keyfilePassword).build();
                        }
                    } else {
                        log.info("Initializing SSL Context with keystore");
                        String keystoreLocation = this.config.getString(NettySourceConnectorConfig.SSL_KEYSTORE_LOCATION_CONF);
                        String keystorePassword = this.config.getPassword(NettySourceConnectorConfig.SSL_KEYSTORE_PASSWORD_CONF).value();
                        String keyPassword = this.config.getPassword(NettySourceConnectorConfig.SSL_KEY_PASSWORD_CONF).value();
                        String alias = this.config.getString(NettySourceConnectorConfig.SSL_KEY_ALIAS_CONF);

                        if (log.isDebugEnabled()) {
                            log.debug("Keystore location: {}", keystoreLocation);
                            log.debug("Keystore password provided: {}", StringUtils.isNotBlank(keystorePassword));
                            log.debug("Key password provided: {}", StringUtils.isNotBlank(keyPassword));
                            log.debug("Key alias: {}", alias);
                        }
                        setupSecurityStores(keystoreLocation, keystorePassword, keystoreType, keyPassword, alias);
                        this.sslContext = SslContextBuilder.forServer(this.privateKey, this.certificateChain).build();
                    }
                }
                SSLEngine engine = sslContext.newEngine(channel.alloc());
                engine.setUseClientMode(false);
                engine.setNeedClientAuth(false);
                pipeline.addFirst("ssl", new SslHandler(engine));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        // Create the metrics instance
        if (null == BytesTcpChannelInitializer.fetcherMetrics) {
            try {
                BytesTcpChannelInitializer.fetcherMetrics = new FetcherMetrics();
                int port = this.config.getInt(NettySourceConnectorConfig.LISTEN_PORT_CONF);

                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                objectName = new ObjectName(
                    String.format(
                        "confluentps.connect.source.netty:type=TcpFetcherMetrics,protocol=%s,port=%d,channel=%s",
                        protocol.toLowerCase(),
                        port,
                        channel.id().asShortText()
                    )
                );
                // See if the mbean is already registered
                if (mbs.isRegistered(objectName)) {
                    log.warn("Fetcher MBean with name {} is already registered. This may be due to a start/stop operation. The existing MBean will be unregistered.", objectName.toString());
                    mbs.unregisterMBean(objectName);
                }
                mbs.registerMBean(BytesTcpChannelInitializer.fetcherMetrics, objectName);
            } catch (Exception e) {
                log.error("Error registering FetcherMetrics MBean: " + e.getMessage(), e);
            }
        }

        if (readTimeoutHandler != null) {
            pipeline.addLast("nodataTimeout", readTimeoutHandler);
        }

        String handlerName = "recordHandler";
        SourceRecordHandler handler = null;
        if ("HTTP".equalsIgnoreCase(protocol) || "HTTPS".equalsIgnoreCase(protocol)) {
            pipeline.addLast("requestDecoder", new HttpRequestDecoder());
            pipeline.addLast("responseEncoder", new HttpResponseEncoder());
            handler = new HttpServerHandler();
        } else {
            int maxLength = this.config.getInt(NettySourceConnectorConfig.MAX_FRAME_LENGTH_CONF);
            pipeline.addLast("framer", new DelimiterBasedFrameDecoder(
                maxLength,
                stripDelimiter,
                failFast,
                this.delimiters.toArray(new ByteBuf[0])
            ));
            pipeline.addLast("decoder", new StringDecoder());

            // Note: value.converter is defined in org.apache.kafka.connect.runtime.WorkerConfig
            String valueConverter = this.config.getString("value.converter");
            if (valueConverter != null && valueConverter.equalsIgnoreCase("org.apache.kafka.connect.storage.StringConverter")) {
                log.debug("Detected org.apache.kafka.connect.storage.StringConverter for value.converter, using StringRecordHandler");
                handler = new StringRecordHandler();
            } else {
                log.debug("Using StructuredRecordHandler for non-StringConverter value.converter");
                handler = new StructuredRecordHandler();
            }
        }
        handler.setTopic(topic);
        handler.setRecordQueue(messageQueue);
        handler.setFetcherMetrics(BytesTcpChannelInitializer.fetcherMetrics);
        pipeline.addLast(handlerName, handler);
    }

    protected void setupSecurityStores(
        String keystoreLocation,
        String keystorePassword,
        String keystoreType,
        String keyPassword,
        String keyAlias
    ) throws KeyStoreException, KeyStoreAccessException, KeyStoreTypeException, InvalidPasswordException, KeyStoreEmptyException, InvalidAliasException {
        KeyStore keyStore = CertificateUtils.loadKeyStore(keystoreLocation, keystoreType, keystorePassword.toCharArray());
        String alias = CertificateUtils.getAliasOrFirstKeyEntry(keyStore, keyAlias);

        this.certificateChain = CertificateUtils.getCertificateChain(keyStore, alias);
        this.privateKey = CertificateUtils.getPrivateKey(keyStore, alias, keyPassword.toCharArray());
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
