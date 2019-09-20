/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.axesoft.tans.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClientApp {

    public static final List<String> URLS = ImmutableList.of(
            "http://192.168.1.100:8081/acquire?key=monkey.id&n=3"
            , "http://192.168.1.100:8083/acquire?key=star.id&n=2"
            , "http://192.168.1.100:8082/acquire?key=pig.id&n=1"
    );

    public static void main(String[] args) throws Exception {
        ClientApp app = new ClientApp();
        app.run(URLS);
    }

    private Map<InetSocketAddress, HttpRequest> requestMap;

    public void run(List<String> urls) throws Exception {
        List<URI> uris = Lists.transform(urls, this::toUri);
        this.requestMap = uris.stream().collect(Collectors.toMap
                (uri -> InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort()),
                        uri -> createRequest(uri)));


        HttpTaskRunner runner = new HttpTaskRunner(this::handleResponse);
        for (String url : urls) {
            URI uri = toUri(url);
            runner.addTask(uri.getHost(), uri.getPort(), this.requestMap.get(InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort())),
                    10, 100000);
        }
        runner.run();
    }

    private static HttpRequest createRequest(URI uri) {
        HttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getPath() + "?" + uri.getQuery(), Unpooled.EMPTY_BUFFER);
        request.headers().set(HttpHeaderNames.HOST, uri.getHost());
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);

        return request;
    }

    private void handleResponse(InetSocketAddress from, HttpResponse response, HttpContent content) {
        //System.out.println("process");
        boolean isRedirect = HttpTaskRunner.isRedirectCode(response.status().code());
        String s = isRedirect ?
                response.headers().get(HttpHeaderNames.LOCATION)
                : content.content().toString(CharsetUtil.UTF_8).lines().findFirst().orElseGet(() -> "");

        String info = String.format("%s, %s, %s [%s]",
                response.headers().get(HttpHeaderNames.HOST),
                response.headers().get(HttpHeaderNames.FROM),
                response.status().codeAsText(),
                s);
        //System.err.println(info);
    }

    private URI toUri(String url) {
        try {
            return new URI(url);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException();
        }
    }
}
