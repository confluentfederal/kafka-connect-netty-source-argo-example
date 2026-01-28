package io.confluent.ps.connect.http;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class HttpUtils {
    private HttpUtils() {}

    /**
     * Thanks Gemini!
     * @param request
     * @return
     */
    public static String prettyPrint(HttpRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("HttpRequest:\n");
        sb.append("  Method: ").append(request.method()).append("\n");
        sb.append("  URI: ").append(request.uri()).append("\n");
        sb.append("  Headers:\n");
        request.headers().map().forEach((name, values) -> {
            for (String value : values) {
                sb.append("    ").append(name).append(": ").append(value).append("\n");
            }
        });
        sb.append("Body:\n");
        if (request.bodyPublisher().isPresent()) {
            try {
                // This part might require specific handling based on the body type
                // The example assumes it's a String, adjust accordingly
                request.bodyPublisher().ifPresent(publisher -> {
                    try {
                        // java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(1024); // Adjust size as needed
                        java.io.ByteArrayOutputStream bout = new java.io.ByteArrayOutputStream();
                        publisher.subscribe(new Flow.Subscriber<java.nio.ByteBuffer>() {
                            @Override
                            public void onSubscribe(Subscription subscription) {
                                subscription.request(Long.MAX_VALUE);
                            }
                            @Override
                            public void onNext(java.nio.ByteBuffer item) {
                                try {
                                    bout.write(item.array());
                                } catch (IOException ex) {
                                    log.error("An error occurred printing the request body", ex);
                                }
                            }
                            @Override
                            public void onError(Throwable throwable) {
                                log.error("An error occurred printing the request body", throwable);
                            }
                            @Override
                            public void onComplete() {
                                sb.append("  ").append(bout.toString(StandardCharsets.UTF_8)).append("\n");
                            }
                        });
                    } catch (Exception e) {
                        sb.append("  (Error reading body)").append("\n");
                    }
                });
            } catch (Exception e) {
                    sb.append("  (Error processing body)").append("\n");
            }
        } else {
            sb.append("  (No body)").append("\n");
        }
        return sb.toString();
    }

    /**
     * Thanks Gemini!
     * @param response
     * @return
     */
    public static String prettyPrint(HttpResponse<?> response, boolean includeBody) {
        StringBuilder sb = new StringBuilder();
        sb.append("HttpResponse:\n");
        sb.append("  Status Code: ").append(response.statusCode()).append("\n");
        sb.append("  Headers:\n");
        response.headers().map().forEach((name, values) -> {
            for (String value : values) {
                sb.append("    ").append(name).append(": ").append(value).append("\n");
            }
        });
        if (includeBody) {
            sb.append("  Body:\n");
            if (response.body() instanceof String) {
                sb.append("    ").append(response.body()).append("\n");
            } else if (response.body() instanceof byte[]) {
                sb.append("    ").append(new String((byte[]) response.body(), StandardCharsets.UTF_8)).append("\n");
            } else {
                sb.append("    (Non-string body type: ").append(response.body().getClass().getName()).append(")\n");
            }
        }
        return sb.toString();
    }
}
