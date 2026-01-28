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

import java.nio.charset.StandardCharsets;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinglePacketHandler extends ChannelInboundHandlerAdapter {
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {

    String data = null;
    if (obj == null) {
      log.trace("NULL message received from: {}", ctx.channel().remoteAddress());
    } else {
      DatagramPacket packet = (DatagramPacket)obj;
      if (log.isTraceEnabled()) {  
        log.trace("Received: {} from {}", obj.getClass(), packet.sender().getAddress());
      }
      // data = packet.content().readBytes(packet.content().readableBytes());
      data = packet.content().toString(StandardCharsets.UTF_8);
      int trim = 0;
      if (data.endsWith("\r\n")) {
        trim = 2;
      } else if (data.endsWith("\n")) {
        trim = 1;
      }
      data = data.substring(0, data.length() - trim);
    }

    ctx.fireChannelRead(data);
  }
}
