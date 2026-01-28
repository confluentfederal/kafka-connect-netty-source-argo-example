package io.confluent.ps.connect.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Simple HTTP client for making requests to fetcher endpoints. Automatically
 * handles gzip encoding.
 */
public class FetcherClient {
    private HttpClient httpClient;

    public FetcherClient() {
        this.httpClient = HttpClient.newBuilder()
                .version(Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(20))
                .followRedirects(Redirect.NORMAL)
                .build();
    }

    public static InputStream getDecodedInputStream(
            HttpResponse<InputStream> httpResponse) {
        String encoding = determineContentEncoding(httpResponse);
        try {
            switch (encoding) {
                case "":
                    return httpResponse.body();
                case "gzip":
                    return new GZIPInputStream(httpResponse.body());
                default:
                    throw new UnsupportedOperationException(
                            "Unexpected Content-Encoding: " + encoding);
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    public static String determineContentEncoding(
            HttpResponse<?> httpResponse) {
        return httpResponse.headers().firstValue("Content-Encoding").orElse("");
    }

    public HttpResponse<InputStream> post(URI uri, String contentType, String postBody) throws IOException, InterruptedException {
        return post(uri, null, contentType, postBody);
    }

    public HttpResponse<InputStream> post(URI uri, Map<String, String> headers, String contentType, String postBody) throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", contentType)
                .header("Accept-Encoding", "gzip")
                .POST(BodyPublishers.ofString(postBody));

        if (null != headers && !headers.isEmpty()) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                requestBuilder.header(header.getKey(), header.getValue());
            }
        }

        HttpRequest request = requestBuilder.build();

        return this.httpClient.send(request, BodyHandlers.ofInputStream());
    }

    public HttpResponse<InputStream> get(URI uri) throws IOException, InterruptedException {
        return get(uri, null);
    }

    public HttpResponse<InputStream> get(URI uri, Map<String, String> headers) throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(30))
                .header("Accept-Encoding", "gzip")
                .GET();

        if (null != headers && !headers.isEmpty()) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                requestBuilder.header(header.getKey(), header.getValue());
            }
        }

        HttpRequest request = requestBuilder.build();

        return this.httpClient.send(request, BodyHandlers.ofInputStream());
    }

}
