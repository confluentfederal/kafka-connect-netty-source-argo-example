package io.confluent.ps.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

public abstract class RequestUtils {
    private RequestUtils() {}

    public static Map<String, List<String>> extractHeaders(HttpHeaders headers) {
        Map<String, List<String>> result = new HashMap<>();
        for (String key : headers.names()) {
            result.put(key, headers.getAll(key));
        }
        return result;
    }

    public static Map<String, List<String>> extractParams(HttpRequest request) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        return params;
    }

    public static String extractBody(HttpContent httpContent) {
        ByteBuf content = httpContent.content();
        if (content.isReadable()) {
            String result = content.toString(CharsetUtil.UTF_8);
            content.release();
            return result;
        }
        return null;
    }

    public static void evaluateDecoderResult(HttpObject o) throws Throwable {
        DecoderResult result = o.decoderResult();

        if (!result.isSuccess()) {
            throw result.cause();
        }
    }

}
