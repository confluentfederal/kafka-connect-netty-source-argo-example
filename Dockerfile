# Dockerfile for Kafka Connect with Netty Source Connector
# This image extends the Confluent Connect image with the Netty connector pre-installed

ARG CP_VERSION=8.1.0

FROM confluentinc/cp-server-connect:${CP_VERSION}

# Copy the connector JARs to the plugins directory
COPY target/*.jar /usr/share/java/kafka-connect-netty-source/

# Set the plugin path to include our connector
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

USER appuser
