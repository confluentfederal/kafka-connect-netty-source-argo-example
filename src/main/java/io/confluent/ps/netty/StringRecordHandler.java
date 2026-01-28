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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringRecordHandler extends SourceRecordHandler {

  private final boolean skipBlank;

  public StringRecordHandler() {
    this.skipBlank = true;
  }

  public StringRecordHandler(boolean skipBlank) {
    this.skipBlank = skipBlank;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
    // log.trace("channelRead: {} -> '{}'", obj.getClass().getName(), obj);

    // Cast to a String first.
    String msg = (String) obj;
    if (skipBlank && StringUtils.isBlank(msg)) {
      return;
    }
    Map<String, ?> sourcePartition = new HashMap<>();
    Map<String, ?> sourceOffset = new HashMap<>();

    if (recordQueue == null) {
      log.error("recordQueue is not configured");
      throw new IllegalStateException("recordQueue is not configured");
    }

    Object value = formatMessage(msg, ctx);
    Schema valueSchema = getValueSchema();
    SourceRecord srcRec = new SourceRecord(sourcePartition, sourceOffset, topic, valueSchema, value);

    //add remoteAddr to headers
    SocketAddress remoteAddr = ctx.channel().remoteAddress();
    if (remoteAddr != null) {
      if (remoteAddr instanceof InetSocketAddress) {
        srcRec.headers().add("remoteHost", new SchemaAndValue(Schema.STRING_SCHEMA, ((InetSocketAddress) remoteAddr).getHostString()));
        srcRec.headers().add("remotePort", new SchemaAndValue(Schema.INT32_SCHEMA, ((InetSocketAddress) remoteAddr).getPort()));
      }
      srcRec.headers().add("remoteAddress", new SchemaAndValue(Schema.STRING_SCHEMA, remoteAddr.toString()));
    }

    //add 'transportProtocol' header
    Channel channel = ctx.channel();
    if (channel != null) {

      if (channel instanceof ServerSocketChannel || channel instanceof NioSocketChannel) {
        srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, "tcp"));
      } else if (channel instanceof DatagramChannel || channel instanceof NioDatagramChannel) {
        srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, "udp"));
      } else {
        // log.info("Channel -> {}", channel.getClass().getName());
        srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, channel.toString()));
      }
    }

    SocketAddress localAddr = ctx.channel().localAddress();
    if (localAddr != null) {
      if (localAddr instanceof InetSocketAddress) {
        srcRec.headers().add("localHost", new SchemaAndValue(Schema.STRING_SCHEMA, ((InetSocketAddress) localAddr).getHostString()));
        srcRec.headers().add("localPort", new SchemaAndValue(Schema.INT32_SCHEMA, ((InetSocketAddress) localAddr).getPort()));
      }
      srcRec.headers().add("localAddress", new SchemaAndValue(Schema.STRING_SCHEMA, localAddr.toString()));
    }
    this.fetcherMetrics.incrementEvent_Count(1);
    recordQueue.add(srcRec);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    this.fetcherMetrics.setLast_Error(cause.getLocalizedMessage());
    ctx.close();
  }

  /**
   * Format the message before creating SourceRecord. The default
   * implementation is an identity function.
   *
   * @param msg
   * @param ctx
   * @return
   */
  protected Object formatMessage(String msg, ChannelHandlerContext ctx) {
    return msg;
  }

  /**
   * Get the Schema for the value. The default implementation returns null.
   *
   * @return
   */
  protected Schema getValueSchema() {
    return null;
  }

}
