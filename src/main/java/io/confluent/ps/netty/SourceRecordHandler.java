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

import java.util.Deque;

import org.apache.kafka.connect.source.SourceRecord;

import io.confluent.ps.connect.metrics.FetcherMetrics;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * Base class for message handling ChannelHandlers
 */
@Slf4j
public class SourceRecordHandler extends ChannelInboundHandlerAdapter {

  protected Deque<SourceRecord> recordQueue;
  protected String topic;
  protected FetcherMetrics fetcherMetrics;

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setRecordQueue(Deque<SourceRecord> queue) {
    this.recordQueue = queue;
  }

  public void setFetcherMetrics(FetcherMetrics fetcherMetrics) {
    this.fetcherMetrics = fetcherMetrics;
  }

}
