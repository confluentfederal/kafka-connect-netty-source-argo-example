package io.confluent.ps.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.netty.channel.ChannelHandlerContext;

public class StructuredRecordHandler extends StringRecordHandler {

  private static final String HOST = "host";
  private static final String REMOTE_ADDRESS = "remoteAddress";
  private static final String RAW_MESSAGE = "rawMessage";
  private static final String RECEIVED_AT = "receivedAt";
  private static final String RECEIVED_EPOCH_MILLIS = "receivedEpochMillis";

  private static final Schema SCHEMA = SchemaBuilder.struct()
      .name("NettySourceMessage").version(1).doc("Network message")
      .field(HOST, Schema.OPTIONAL_STRING_SCHEMA)
      .field(REMOTE_ADDRESS, Schema.OPTIONAL_STRING_SCHEMA)
      .field(RAW_MESSAGE, Schema.STRING_SCHEMA)
      .field(RECEIVED_AT, Schema.STRING_SCHEMA)
      .field(RECEIVED_EPOCH_MILLIS, Schema.INT64_SCHEMA)
      .build();

  /**
   * Get the Schema for the value loosely based on the Syslog Source Connector.
   *
   * @return
   */
  protected Schema getValueSchema() {
    return SCHEMA;
  }

  /**
   * Convert the incoming String message to a structured object loosely based
   * on the Syslog Source Connector.
   *
   * @param msg
   * @param ctx
   * @return
   */
  protected Object formatMessage(String msg, ChannelHandlerContext ctx) {
    Instant now = Instant.now();
    Struct struct = new Struct(SCHEMA);
    SocketAddress localAddr = ctx.channel().localAddress();
    if (localAddr != null) {
      if (localAddr instanceof InetSocketAddress) {
        struct.put(HOST, ((InetSocketAddress) localAddr).getHostString());
      }
    }
    SocketAddress remoteAddr = ctx.channel().remoteAddress();
    if (remoteAddr != null) {
      if (remoteAddr instanceof InetSocketAddress) {
        struct.put(REMOTE_ADDRESS, ((InetSocketAddress) remoteAddr).getHostString());
      }
    }
    struct.put(RAW_MESSAGE, msg);
    struct.put(RECEIVED_AT, now.toString());
    struct.put(RECEIVED_EPOCH_MILLIS, now.toEpochMilli());

    return struct;
  }

}
