package io.confluent.ps.netty;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.confluent.ps.utils.RequestUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpServerHandler extends SourceRecordHandler {
    private HttpRequest request;
    private Map<String, Object> responseData = new HashMap<>();
    private StringBuilder body;

    private static final String PARAMETERS = "parameters";
    private static final String BODY = "body";
    private static final String ERROR = "error";
    private static final String METHOD = "method";
    private static final String URI = "uri";
    private static final String PROTOCOL_VERSION = "protocol_version";
    private static final String HEADERS = "headers";

    private static Schema schema;

    static {
        schema = SchemaBuilder.struct()
            .name("io.confluent.ps.HttpRequest").version(1).doc("HTTP request")
            .field(ERROR, Schema.OPTIONAL_STRING_SCHEMA)
            .field(URI, Schema.STRING_SCHEMA)
            .field(PROTOCOL_VERSION, Schema.STRING_SCHEMA)
            .field(METHOD, Schema.STRING_SCHEMA)
            .field(PARAMETERS,
                SchemaBuilder.map(
                    Schema.STRING_SCHEMA,
                    SchemaBuilder.array(
                        Schema.STRING_SCHEMA
                    )
                )
                .optional()
                .build()
            )
            .field(HEADERS,
                SchemaBuilder.map(
                    Schema.STRING_SCHEMA,
                    SchemaBuilder.array(
                        Schema.STRING_SCHEMA
                    )
                )
                .optional()
                .build()
            )
            .field(BODY, Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Start the chain of calls
        if (msg instanceof HttpRequest) {
            HttpRequest request = this.request = (HttpRequest) msg;

            if (HttpUtil.is100ContinueExpected(request)) {
                writeResponse(ctx);
            }
            responseData.clear();
            responseData.put(PARAMETERS, RequestUtils.extractParams(request));
            responseData.put(METHOD, request.method().name());
            responseData.put(URI, request.uri());
            responseData.put(PROTOCOL_VERSION, request.protocolVersion().text());
            responseData.put(HEADERS, RequestUtils.extractHeaders(request.headers()));
            body = new StringBuilder();
        }

        try {
            RequestUtils.evaluateDecoderResult(request);
        } catch (Throwable t) {
            StringWriter writer = new StringWriter();
            PrintWriter pw = new PrintWriter(writer);
            t.printStackTrace(pw);
            responseData.put(ERROR, writer.toString());
        }

        if (msg instanceof HttpContent) {
            HttpContent httpContent = (HttpContent) msg;
            body.append(RequestUtils.extractBody(httpContent));

            if (msg instanceof LastHttpContent) {
                responseData.put(BODY, RequestUtils.extractBody(httpContent));
                LastHttpContent trailer = (LastHttpContent) msg;
                if (!trailer.trailingHeaders().isEmpty()) {
                    Map<String, List<String>> newHeaders = RequestUtils.extractHeaders(trailer.trailingHeaders());
                    Map<String, List<String>> headers = (Map<String, List<String>>) responseData.get(HEADERS);
                    for (Map.Entry<String,List<String>> entry : newHeaders.entrySet()) {
                        if (headers.containsKey(entry.getKey())) {
                            headers.get(entry.getKey()).addAll(entry.getValue());
                        } else {
                            headers.put(entry.getKey(), entry.getValue());
                        }
                    }
                    responseData.put(HEADERS, headers);
                }

                writeResponse(ctx, trailer, responseData);
            }
        }
    }

    protected void writeResponse(ChannelHandlerContext ctx, LastHttpContent trailer,
            Map<String, Object> responseData) {
        log.debug("Writing default response");
        boolean keepAlive = HttpUtil.isKeepAlive(request);
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                ((HttpObject) trailer).decoderResult().isSuccess() ? HttpResponseStatus.OK
                        : HttpResponseStatus.BAD_REQUEST,
                Unpooled.EMPTY_BUFFER);

        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        if (keepAlive) {
            httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                    httpResponse.content().readableBytes());
            httpResponse.headers().set(HttpHeaderNames.CONNECTION,
                    HttpHeaderValues.KEEP_ALIVE);
        }
        ctx.write(httpResponse);

        if (!keepAlive) {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }

        writeRequestToQueue(ctx, responseData);
    }

    protected Struct convert(Map<String, Object> msg) {
        Struct struct = new Struct(schema);
        struct.put(ERROR, msg.get(ERROR));
        struct.put(URI, msg.get(URI));
        struct.put(PROTOCOL_VERSION, msg.get(PROTOCOL_VERSION));
        struct.put(METHOD, msg.get(METHOD));
        struct.put(PARAMETERS, msg.get(PARAMETERS));
        struct.put(HEADERS, msg.get(HEADERS));
        struct.put(BODY, msg.get(BODY));

        return struct;
    }

    protected void writeRequestToQueue(ChannelHandlerContext ctx, Map<String, Object> msg) {
        log.debug("Writing message to queue");
        // Write the requestData to the queue
        Map<String, ?> sourcePartition = new HashMap<>();
        Map<String, ?> sourceOffset = new HashMap<>();

        if (this.recordQueue == null) {
            log.error("recordQueue is not configured");
            throw new IllegalStateException("recordQueue is not configured");
        }

        SourceRecord srcRec = new SourceRecord(
            sourcePartition, sourceOffset, topic, schema,
            convert(msg));

        // add remoteAddr to headers
        SocketAddress remoteAddr = ctx.channel().remoteAddress();
        if (remoteAddr != null) {
            if (remoteAddr instanceof InetSocketAddress) {
                srcRec.headers().add("remoteHost",
                        new SchemaAndValue(Schema.STRING_SCHEMA, ((InetSocketAddress) remoteAddr).getHostString()));
                srcRec.headers().add("remotePort",
                        new SchemaAndValue(Schema.INT32_SCHEMA, ((InetSocketAddress) remoteAddr).getPort()));
            }
            srcRec.headers().add("remoteAddress", new SchemaAndValue(Schema.STRING_SCHEMA, remoteAddr.toString()));
        }

        // add 'transportProtocol' header
        Channel channel = ctx.channel();
        if (channel != null) {
            if (channel instanceof ServerSocketChannel || channel instanceof NioSocketChannel) {
                srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, "tcp"));
            } else if (channel instanceof DatagramChannel || channel instanceof NioDatagramChannel) {
                srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, "udp"));
            } else {
                srcRec.headers().add("transportProtocol", new SchemaAndValue(Schema.STRING_SCHEMA, channel.toString()));
            }
        }

        SocketAddress localAddr = ctx.channel().localAddress();
        if (localAddr != null) {
            if (localAddr instanceof InetSocketAddress) {
                srcRec.headers().add("localHost",
                        new SchemaAndValue(Schema.STRING_SCHEMA, ((InetSocketAddress) localAddr).getHostString()));
                srcRec.headers().add("localPort",
                        new SchemaAndValue(Schema.INT32_SCHEMA, ((InetSocketAddress) localAddr).getPort()));
            }
            srcRec.headers().add("localAddress", new SchemaAndValue(Schema.STRING_SCHEMA, localAddr.toString()));
        }
        this.fetcherMetrics.incrementEvent_Count(1);
        recordQueue.add(srcRec);
    }

    private void writeResponse(ChannelHandlerContext ctx) {
        log.debug("Writing default CONTINUE response");
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE,
                Unpooled.EMPTY_BUFFER);
        ctx.write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        this.fetcherMetrics.setLast_Error(cause.getLocalizedMessage());
        ctx.close();
    }
}
