package io.confluent.ps.netty;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import lombok.extern.slf4j.Slf4j;

public class NettySourceConnectorConfig extends AbstractConfig{

    private static final String PROPERTIES_GROUP = "Netty Source Properties";

    public static final String KAFKA_TOPIC_CONF = "kafka.topic";
    private static final String KAFKA_TOPIC_DOC = "Topic to write to";
    private static final String KAFKA_TOPIC_DISPLAY = "Target topic";

    public static final String VALUE_CONVERTER_CONF = "value.converter";
    private static final String VALUE_CONVERTER_DOC = "Converter class used to convert between Kafka Connect format and the serialized form that is written to Kafka. This controls the format of the values in messages written to or read from Kafka, and since this is independent of connectors it allows any connector to work with any serialization format. Examples of common formats include JSON and Avro.";
    private static final String VALUE_CONVERTER_DISPLAY = "Value converter class";
    private static final String VALUE_CONVERTER_DEFAULT = "org.apache.kafka.connect.storage.StringConverter";

    public static final String BIND_ADDRESS_CONF = "netty.bind.address";
    private static final String BIND_ADDRESS_DOC = "Network address to bind to";
    private static final String BIND_ADDRESS_DISPLAY = "Bind address";
    private static final String BIND_ADDRESS_DEFAULT = "0.0.0.0";

    public static final String LISTEN_PORT_CONF = "netty.listen.port";
    private static final String LISTEN_PORT_DOC = "Port to listen on";
    private static final String LISTEN_PORT_DISPLAY = "Listen port";

    public static final String PROTOCOL_CONF = "netty.protocol";
    private static final String PROTOCOL_DOC = "Transport protocol";
    private static final String PROTOCOL_DISPLAY = "Transport protocol";
    private static final String PROTOCOL_DEFAULT = "tcp";

    // public static final String WORKER_THREAD_COUNT_CONF = "netty.worker.thread.count";
    // private static final String WORKER_THREAD_COUNT_DOC = "Worker thread count";
    // private static final String WORKER_THREAD_COUNT_DISPLAY = "Worker thread count";
    // private static final int WORKER_THREAD_COUNT_DEFAULT = Runtime.getRuntime().availableProcessors();

    public static final String MAX_FRAME_LENGTH_CONF = "netty.max.frame.length";
    private static final String MAX_FRAME_LENGTH_DOC = "Maximum length of a frame (for TCP protocols)";
    private static final String MAX_FRAME_LENGTH_DISPLAY = "Max frame length";
    private static final int MAX_FRAME_LENGTH_DEFAULT = 100 * 1024; // 100 KB

    public static final String SSL_CERTFILE_LOCATION_CONF = "netty.ssl.certfile.location";
    private static final String SSL_CERTFILE_LOCATION_DOC = "Certificate chain location (combined PEM)";
    private static final String SSL_CERTFILE_LOCATION_DISPLAY = "Certificate chain location (combined PEM)";
    private static final String SSL_CERTFILE_LOCATION_DEFAULT = "";

    public static final String SSL_KEYFILE_LOCATION_CONF = "netty.ssl.keyfile.location";
    private static final String SSL_KEYFILE_LOCATION_DOC = "Certificate key file location";
    private static final String SSL_KEYFILE_LOCATION_DISPLAY = "Certificate key file location";
    private static final String SSL_KEYFILE_LOCATION_DEFAULT = "";

    public static final String SSL_KEYFILE_PASSWORD_CONF = "netty.ssl.keyfile.password";
    private static final String SSL_KEYFILE_PASSWORD_DOC = "Certificate key file password (optional)";
    private static final String SSL_KEYFILE_PASSWORD_DISPLAY = "Certificate key file password (optional)";
    private static final String SSL_KEYFILE_PASSWORD_DEFAULT = "";

    public static final String SSL_KEYSTORE_LOCATION_CONF = "netty.ssl.keystore.location";
    private static final String SSL_KEYSTORE_LOCATION_DOC = "Certificate keystore location";
    private static final String SSL_KEYSTORE_LOCATION_DISPLAY = "Certificate keystore location";
    private static final String SSL_KEYSTORE_LOCATION_DEFAULT = "";

    public static final String SSL_KEYSTORE_PASSWORD_CONF = "netty.ssl.keystore.password";
    private static final String SSL_KEYSTORE_PASSWORD_DOC = "Certificate keystore password";
    private static final String SSL_KEYSTORE_PASSWORD_DISPLAY = "Certificate keystore password";
    private static final String SSL_KEYSTORE_PASSWORD_DEFAULT = "";

    public static final String SSL_KEYSTORE_TYPE_CONF = "netty.ssl.keystore.type";
    private static final String SSL_KEYSTORE_TYPE_DOC = "Certificate keystore type";
    private static final String SSL_KEYSTORE_TYPE_DISPLAY = "Certificate keystore type";
    private static final String SSL_KEYSTORE_TYPE_DEFAULT = "PKCS12";

    public static final String SSL_KEY_PASSWORD_CONF = "netty.ssl.key.password";
    private static final String SSL_KEY_PASSWORD_DOC = "Certificate key password";
    private static final String SSL_KEY_PASSWORD_DISPLAY = "Certificate key password";
    private static final String SSL_KEY_PASSWORD_DEFAULT = "";

    public static final String SSL_KEY_ALIAS_CONF = "netty.ssl.key.alias";
    private static final String SSL_KEY_ALIAS_DOC = "Certificate alias. If provided then this alias will be used to select the certificate in the keystore.";
    private static final String SSL_KEY_ALIAS_DISPLAY = "Certificate alias";
    private static final String SSL_KEY_ALIAS_DEFAULT = "";

    public static final String QUEUE_CAPACITY_CONF = "netty.queue.capacity";
    private static final String QUEUE_CAPACITY_DOC = "Size of the producer queue";
    private static final String QUEUE_CAPACITY_DISPLAY = "Queue capacity";
    private static final int QUEUE_CAPACITY_DEFAULT = 50000;

    public static final String QUEUE_BATCH_SIZE_CONF = "netty.queue.batch.size";
    private static final String QUEUE_BATCH_SIZE_DOC = "Number of records to drain at a time";
    private static final String QUEUE_BATCH_SIZE_DISPLAY = "Queue drain batch size";
    private static final int QUEUE_BATCH_SIZE_DEFAULT = 1000;

    public static final String QUEUE_EMPTY_WAIT_MS_CONF = "netty.queue.empty.wait.ms";
    private static final String QUEUE_EMPTY_WAIT_MS_DOC = "Time in milliseconds to wait if no records were drained";
    private static final String QUEUE_EMPTY_WAIT_MS_DISPLAY = "Queue empty wait ms";
    private static final int QUEUE_EMPTY_WAIT_MS_DEFAULT = 100;

    public static final String QUEUE_CAPACITY_TIMEOUT_MS_CONF = "netty.queue.capacity.timeout.ms";
    private static final String QUEUE_CAPACITY_TIMEOUT_MS_DOC = "Time in milliseconds maximum to wait for more capacity before throwing an exception";
    private static final String QUEUE_CAPACITY_TIMEOUT_MS_DISPLAY = "Queue capacity timeout ms";
    private static final int QUEUE_CAPACITY_TIMEOUT_MS_DEFAULT = 100 * 60;

    public static final String QUEUE_CAPACITY_WAIT_MS_CONF = "netty.queue.capacity.wait.ms";
    private static final String QUEUE_CAPACITY_WAIT_MS_DOC = "Time in milliseconds to wait for more capacity";
    private static final String QUEUE_CAPACITY_WAIT_MS_DISPLAY = "Queue capacity wait time ms";
    private static final int QUEUE_CAPACITY_WAIT_MS_DEFAULT = 100;

    public NettySourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public NettySourceConnectorConfig(Map<String, String> parsedConfig) {
        this(config(), parsedConfig);
    }

    public static ConfigDef config() {
        int position = 0;

        return new ConfigDef()
            .define(VALUE_CONVERTER_CONF, Type.STRING, VALUE_CONVERTER_DEFAULT, Importance.HIGH, VALUE_CONVERTER_DOC, "Common", 4, Width.LONG, VALUE_CONVERTER_DISPLAY)

            .define(KAFKA_TOPIC_CONF, Type.STRING, Importance.HIGH, KAFKA_TOPIC_DOC, PROPERTIES_GROUP, ++position, Width.LONG, KAFKA_TOPIC_DISPLAY)
            .define(BIND_ADDRESS_CONF, Type.STRING, BIND_ADDRESS_DEFAULT, Importance.HIGH, BIND_ADDRESS_DOC, PROPERTIES_GROUP, ++position, Width.LONG, BIND_ADDRESS_DISPLAY)
            .define(LISTEN_PORT_CONF, Type.INT, Importance.HIGH, LISTEN_PORT_DOC, PROPERTIES_GROUP, ++position, Width.LONG, LISTEN_PORT_DISPLAY)
            .define(PROTOCOL_CONF, Type.STRING, PROTOCOL_DEFAULT, Importance.HIGH, PROTOCOL_DOC, PROPERTIES_GROUP, ++position, Width.LONG, PROTOCOL_DISPLAY, new TransportProtocolRecommender())

            .define(SSL_KEYSTORE_TYPE_CONF, Type.STRING, SSL_KEYSTORE_TYPE_DEFAULT, Importance.MEDIUM, SSL_KEYSTORE_TYPE_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_KEYSTORE_TYPE_DISPLAY, new KeyStoreTypeRecommender())

            .define(SSL_KEYSTORE_LOCATION_CONF, Type.STRING, SSL_KEYSTORE_LOCATION_DEFAULT, Importance.MEDIUM, SSL_KEYSTORE_LOCATION_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_KEYSTORE_LOCATION_DISPLAY, new VisibleOnNonPemRecommender(false))
            .define(SSL_KEYSTORE_PASSWORD_CONF, Type.PASSWORD, SSL_KEYSTORE_PASSWORD_DEFAULT, Importance.MEDIUM, SSL_KEYSTORE_PASSWORD_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_KEYSTORE_PASSWORD_DISPLAY, new VisibleOnNonPemRecommender(false))
            .define(SSL_KEY_PASSWORD_CONF, Type.PASSWORD, SSL_KEY_PASSWORD_DEFAULT, Importance.MEDIUM, SSL_KEY_PASSWORD_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_KEY_PASSWORD_DISPLAY, new VisibleOnNonPemRecommender(false))
            .define(SSL_KEY_ALIAS_CONF, Type.STRING, SSL_KEY_ALIAS_DEFAULT, Importance.MEDIUM, SSL_KEY_ALIAS_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_KEY_ALIAS_DISPLAY, new VisibleOnNonPemRecommender(false))

            .define(SSL_CERTFILE_LOCATION_CONF, Type.STRING, SSL_CERTFILE_LOCATION_DEFAULT, Importance.MEDIUM, SSL_CERTFILE_LOCATION_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_CERTFILE_LOCATION_DISPLAY, new VisibleOnNonPemRecommender(true))
            .define(SSL_KEYFILE_LOCATION_CONF, Type.STRING, SSL_KEYFILE_LOCATION_DEFAULT, Importance.MEDIUM, SSL_KEYFILE_LOCATION_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_KEYFILE_LOCATION_DISPLAY, new VisibleOnNonPemRecommender(true))
            .define(SSL_KEYFILE_PASSWORD_CONF, Type.PASSWORD, SSL_KEYFILE_PASSWORD_DEFAULT, Importance.MEDIUM, SSL_KEYFILE_PASSWORD_DOC, PROPERTIES_GROUP, ++position, Width.LONG, SSL_KEYFILE_PASSWORD_DISPLAY, new VisibleOnNonPemRecommender(true))

            // .define(WORKER_THREAD_COUNT_CONF, Type.INT, WORKER_THREAD_COUNT_DEFAULT, Importance.HIGH, WORKER_THREAD_COUNT_DOC, PROPERTIES_GROUP, ++position, Width.LONG, WORKER_THREAD_COUNT_DISPLAY)
            .define(MAX_FRAME_LENGTH_CONF, Type.INT, MAX_FRAME_LENGTH_DEFAULT, Importance.MEDIUM, MAX_FRAME_LENGTH_DOC, PROPERTIES_GROUP, ++position, Width.LONG, MAX_FRAME_LENGTH_DISPLAY)
            .define(QUEUE_CAPACITY_CONF, Type.INT, QUEUE_CAPACITY_DEFAULT, Importance.LOW, QUEUE_CAPACITY_DOC, PROPERTIES_GROUP, ++position, Width.LONG, QUEUE_CAPACITY_DISPLAY)
            .define(QUEUE_BATCH_SIZE_CONF, Type.INT, QUEUE_BATCH_SIZE_DEFAULT, Importance.LOW, QUEUE_BATCH_SIZE_DOC, PROPERTIES_GROUP, ++position, Width.LONG, QUEUE_BATCH_SIZE_DISPLAY)
            .define(QUEUE_EMPTY_WAIT_MS_CONF, Type.INT, QUEUE_EMPTY_WAIT_MS_DEFAULT, Importance.LOW, QUEUE_EMPTY_WAIT_MS_DOC, PROPERTIES_GROUP, ++position, Width.LONG, QUEUE_EMPTY_WAIT_MS_DISPLAY)
            .define(QUEUE_CAPACITY_TIMEOUT_MS_CONF, Type.INT, QUEUE_CAPACITY_TIMEOUT_MS_DEFAULT, Importance.LOW, QUEUE_CAPACITY_TIMEOUT_MS_DOC, PROPERTIES_GROUP, ++position, Width.LONG, QUEUE_CAPACITY_TIMEOUT_MS_DISPLAY)
            .define(QUEUE_CAPACITY_WAIT_MS_CONF, Type.INT, QUEUE_CAPACITY_WAIT_MS_DEFAULT, Importance.LOW, QUEUE_CAPACITY_WAIT_MS_DOC, PROPERTIES_GROUP, ++position, Width.LONG, QUEUE_CAPACITY_WAIT_MS_DISPLAY);
    }

    private static class TransportProtocolRecommender implements Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return List.of("tcp", "udp", "tcpssl", "http", "https");
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    @Slf4j
    private static class KeyStoreTypeRecommender implements Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return List.of("JKS", "PKCS12", "JCEKS", "PEM");
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            Object protocol = parsedConfig.get(PROTOCOL_CONF);

            boolean visible = protocol != null && ("tcpssl".equalsIgnoreCase(protocol.toString()) || "https".equalsIgnoreCase(protocol.toString()));
            // log.info("Visible[{}]: protocol={} -> {}", name, protocol, visible);
            return visible;
        }
    }

    @Slf4j
    private static class VisibleOnNonPemRecommender implements Recommender {
        private final boolean invert;

        public VisibleOnNonPemRecommender(boolean invert) {
            this.invert = invert;
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return List.of();
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            Object protocolValue = parsedConfig.get(PROTOCOL_CONF);
            if (protocolValue == null || (!"tcpssl".equalsIgnoreCase(protocolValue.toString()) && !"https".equalsIgnoreCase(protocolValue.toString()))) {
                return false;
            }

            Object keystoreTypeValue = parsedConfig.get(SSL_KEYSTORE_TYPE_CONF);
            boolean visible = keystoreTypeValue != null && !"PEM".equalsIgnoreCase(keystoreTypeValue.toString());
            return this.invert ? !visible : visible;
        }
    }
}
