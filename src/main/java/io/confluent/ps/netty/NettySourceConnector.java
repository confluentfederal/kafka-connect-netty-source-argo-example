package io.confluent.ps.netty;

import java.io.File;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import io.confluent.ps.utils.CertificateUtils;
import io.confluent.ps.utils.VersionUtil;
import io.netty.handler.ssl.AbstractSslContextProxy;
import io.confluent.ps.utils.CertificateUtils.InvalidAliasException;
import io.confluent.ps.utils.CertificateUtils.InvalidPasswordException;
import io.confluent.ps.utils.CertificateUtils.KeyStoreAccessException;
import io.confluent.ps.utils.CertificateUtils.KeyStoreEmptyException;
import io.confluent.ps.utils.CertificateUtils.KeyStoreTypeException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettySourceConnector extends SourceConnector {

    private NettySourceConnectorConfig config;
    private Class<? extends Task> taskClass;

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    @Override
    public ConfigDef config() {
        return NettySourceConnectorConfig.config();
    }

    @Override
    @SuppressWarnings({ "java:S1149" })
    public void start(Map<String, String> props) {
        // log.info("NettySourceConnector properties:\n{}\n", String.join("\n",
        // props.entrySet().stream().map(entry -> String.format("\t%s: %s",
        // entry.getKey(), entry.getValue())).collect(Collectors.toList())));
        try {
            // Test the properties. If there's an error then an exception will be thrown
            config = new NettySourceConnectorConfig(props);

            String protocol = config.getString(NettySourceConnectorConfig.PROTOCOL_CONF);
            switch (protocol.toUpperCase()) {
                case "TCP":
                case "TCPSSL":
                case "HTTP":
                case "HTTPS":
                    taskClass = NettyTcpSourceTask.class;
                    break;

                case "UDP":
                    taskClass = NettyUdpSourceTask.class;
                    break;

                default:
                    throw new ConfigException("Unknown protocol: " + protocol);
            }
        } catch (Exception e) {
            throw new ConfigException(
                    "Connector could not start because of an error in the configuration: ",
                    e);
        }
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);

        // Validate SSL configuration
        ConfigValue protocol = getConfigValue(config, NettySourceConnectorConfig.PROTOCOL_CONF);
        switch (protocol.value().toString().toUpperCase()) {
            case "TCP":
            case "UDP":
            case "HTTP":
                break;
            case "TCPSSL":
            case "HTTPS":
                config = validateCertificate(config);
                break;
            default:
                return addErrorMesage(config, protocol, "Unknown protocol '" + protocol.value() + "'", null);
        }

        return config;
    }

    protected Config validateCertificate(Config config) {
        ConfigValue keyStoreTypeValue = getConfigValue(config, NettySourceConnectorConfig.SSL_KEYSTORE_TYPE_CONF);
        if (null == keyStoreTypeValue.value() || StringUtils.isBlank(keyStoreTypeValue.value().toString())) {
            return addErrorMesage(
                config, keyStoreTypeValue,
                "SSL keystore type must be provided when using an TLS protocol",
                null);
        }

        if ("pem".equalsIgnoreCase(keyStoreTypeValue.value().toString())) {
            ConfigValue certfileLocationValue = getConfigValue(
                config,
                NettySourceConnectorConfig.SSL_CERTFILE_LOCATION_CONF);
            ConfigValue keyfileLocationValue = getConfigValue(
                config,
                NettySourceConnectorConfig.SSL_KEYFILE_LOCATION_CONF);
            ConfigValue keyfilePasswordValue = getConfigValue(
                config,
                NettySourceConnectorConfig.SSL_KEYFILE_PASSWORD_CONF);

            if (null == certfileLocationValue.value() || StringUtils.isBlank(certfileLocationValue.value().toString())) {
                return addErrorMesage(
                    config, certfileLocationValue,
                    "SSL certificate file location must be provided when using PEM format",
                    null);
            }
            try {
                AbstractSslContextProxy._toX509Certificates(new File(certfileLocationValue.value().toString()));
            } catch (Exception e) {
                return addErrorMesage(
                    config, certfileLocationValue,
                    "File does not contain valid certificates: " + certfileLocationValue.value(),
                    null);
            }

            if (null == keyfileLocationValue.value() || StringUtils.isBlank(keyfileLocationValue.value().toString())) {
                return addErrorMesage(
                    config, keyfileLocationValue,
                    "SSL key file location must be provided when using PEM format",
                    null);
            }
            String keyPassword = null;
            if (null != keyfilePasswordValue.value() && StringUtils.isNotBlank(((Password)(keyfilePasswordValue.value())).value().toString())) {
                keyPassword = ((Password)(keyfilePasswordValue.value())).value().toString();
            }
            try {
                AbstractSslContextProxy._toPrivateKey(new File(keyfileLocationValue.value().toString()), keyPassword);
            } catch (Exception e) {
                return addErrorMesage(
                    config, keyfileLocationValue,
                    "File does not contain valid private key or password is incorrect: " + keyfileLocationValue.value(),
                    null);
            }
        } else {
            ConfigValue keyStoreLocationValue = getConfigValue(
                    config,
                    NettySourceConnectorConfig.SSL_KEYSTORE_LOCATION_CONF);
            ConfigValue keyStorePasswordValue = getConfigValue(
                    config,
                    NettySourceConnectorConfig.SSL_KEYSTORE_PASSWORD_CONF);

            KeyStore keyStore = null;
            try {
                keyStore = CertificateUtils.loadKeyStore(
                        keyStoreLocationValue.value().toString(),
                        keyStoreTypeValue.value().toString(),
                        ((Password) keyStorePasswordValue.value()).value().toCharArray());
            } catch (KeyStoreAccessException ex) {
                return addErrorMesage(
                        config, keyStoreLocationValue,
                        "The client keystore '" + keyStoreLocationValue.value() + "' could not be read",
                        ex);
            } catch (KeyStoreTypeException ex) {
                return addErrorMesage(
                        config, keyStoreTypeValue,
                        "The keystore type '" + keyStoreTypeValue.value() + "' could not be instantiated",
                        ex);
            } catch (InvalidPasswordException ex) {
                return addErrorMesage(
                        config, keyStorePasswordValue,
                        "The keystore could not be accessed using the password provided",
                        ex);
            }

            ConfigValue keyAliasValue = getConfigValue(
                    config,
                    NettySourceConnectorConfig.SSL_KEY_ALIAS_CONF);
            // Scan for the first entry with a private key or the alias
            String alias = null;
            try {
                alias = CertificateUtils.getAliasOrFirstKeyEntry(
                        keyStore,
                        keyAliasValue.value().toString());
            } catch (KeyStoreEmptyException ex) {
                return addErrorMesage(
                        config, keyStoreLocationValue,
                        "Keystore is empty",
                        null);
            } catch (InvalidAliasException ex) {
                return addErrorMesage(config, keyAliasValue, ex.getLocalizedMessage(), ex);
            }

            // Ensure the certificate chain is good
            try {
                CertificateUtils.getCertificateChain(keyStore, alias);
            } catch (KeyStoreException ex) {
                return addErrorMesage(
                        config, keyAliasValue,
                        "The certificate chain associated with the alias is invalid",
                        ex);
            }

            ConfigValue keyPasswordValue = getConfigValue(
                    config,
                    NettySourceConnectorConfig.SSL_KEY_PASSWORD_CONF);
            try {
                String keyPassword = ((Password) keyPasswordValue.value()).value();
                CertificateUtils.getPrivateKey(
                        keyStore,
                        alias,
                        keyPassword.toCharArray());
            } catch (Exception ex) {
                return addErrorMesage(
                        config, keyPasswordValue,
                        "The key could not be accessed using the key password provided",
                        ex);
            }
        }

        return config;
    }

    protected Config addErrorMesage(Config config, ConfigValue configValue, String msg, @Nullable Throwable ex) {
        if (null != ex) {
            log.error(msg, ex);
        } else {
            log.error(msg);
        }
        configValue.addErrorMessage(msg);
        return config;
    }

    public static ConfigValue getConfigValue(Config config, String configName) {
        return config.configValues().stream()
                .filter(value -> value.name().equals(configName))
                .findFirst().get();
    }

    @Override
    public void stop() {
        // NOP
    }

    @Override
    public Class<? extends Task> taskClass() {
        return taskClass;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(this.config.originalsStrings());

        // Just return a single task config because there is only 1 task that should be
        // generated
        return Collections.singletonList(taskProps);
    }

}
