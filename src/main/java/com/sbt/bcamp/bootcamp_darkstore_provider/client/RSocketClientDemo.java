package com.sbt.bcamp.bootcamp_darkstore_provider.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;

/**
 * RSocket Client Demo - Demonstrates all 4 RSocket interaction models
 * using Spring's RSocketRequester (recommended approach).
 */
@Component
@Order(1)
@Slf4j
public class RSocketClientDemo implements CommandLineRunner {

    private static final String RSOCKET_HOST = "localhost";
    private static final int RSOCKET_PORT = 7000;

    private final RSocketRequester requester;

    public RSocketClientDemo(RSocketRequester.Builder builder) {
        this.requester = builder
                .websocket(URI.create("ws://" + RSOCKET_HOST + ":" + RSOCKET_PORT));
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("========================================");
        log.info("RSocket Client Demo - Starting...");
        log.info("========================================");

        // Đợi server sẵn sàng
        Thread.sleep(3000);

        // =====================================================
        // 1. REQUEST-RESPONSE DEMO
        // =====================================================
        log.info("\n>>> DEMO 1: REQUEST-RESPONSE MODE");
        log.info("=================================");

        Mono<Void> demo1 = requester.route("request.good")
                .data("{\"id\":1}")
                .retrieveMono(String.class)
                .doOnNext(r -> log.info("  [REQUEST-RESPONSE] Good #1: {}", r))
                .doOnError(e -> log.error("  [REQUEST-RESPONSE] Error getting good: {}", e.getMessage()))
                .onErrorReturn("{\"error\":\"Not found or error\"}")
                .then();

        Mono<Void> demo1b = requester.route("request.darkstore")
                .data("{\"id\":1}")
                .retrieveMono(String.class)
                .doOnNext(r -> log.info("  [REQUEST-RESPONSE] Darkstore #1: {}", r))
                .doOnError(e -> log.error("  [REQUEST-RESPONSE] Error: {}", e.getMessage()))
                .onErrorReturn("{\"error\":\"Not found\"}")
                .then();

        Mono<Void> demo1c = requester.route("request.delivery")
                .data("{\"id\":1}")
                .retrieveMono(String.class)
                .doOnNext(r -> log.info("  [REQUEST-RESPONSE] Delivery #1: {}", r))
                .doOnError(e -> log.error("  [REQUEST-RESPONSE] Error: {}", e.getMessage()))
                .onErrorReturn("{\"error\":\"Not found\"}")
                .then();

        // Test error handling - request non-existent good
        Mono<Void> demo1d = requester.route("request.good")
                .data("{\"id\":99999}")
                .retrieveMono(String.class)
                .doOnNext(r -> log.info("  [REQUEST-RESPONSE] Non-existent good (error handled): {}", r))
                .doOnError(e -> log.error("  [REQUEST-RESPONSE] Expected error for non-existent good: {}", e.getMessage()))
                .onErrorReturn("{\"error\":\"Handled - record not found\"}")
                .then();

        log.info("  [REQUEST-RESPONSE] Sending requests...");

        // =====================================================
        // 2. REQUEST-STREAM DEMO
        // =====================================================
        log.info("\n>>> DEMO 2: REQUEST-STREAM MODE");
        log.info("================================");

        Flux<String> demo2 = requester.route("stream.goods")
                .data("{}")
                .retrieveFlux(String.class)
                .doOnSubscribe(s -> log.info("  [REQUEST-STREAM] Stream subscription started"))
                .doOnNext(r -> log.info("  [REQUEST-STREAM] Good: {}", r))
                .doOnComplete(() -> log.info("  [REQUEST-STREAM] Stream completed"))
                .doOnError(e -> log.error("  [REQUEST-STREAM] Stream error: {}", e.getMessage()))
                .onErrorResume(e -> {
                    log.error("  [REQUEST-STREAM] Error: {}", e.getMessage());
                    return Flux.just("Stream error: " + e.getMessage());
                });

        log.info("  [REQUEST-STREAM] Starting stream: Getting all goods with delay...");

        // =====================================================
        // 3. REQUEST-STREAM: Price Updates
        // =====================================================
        log.info("\n>>> DEMO 3: REQUEST-STREAM - PRICE UPDATES");
        log.info("============================================");

        Flux<String> demo3 = requester.route("stream.price.updates")
                .data("{\"id\":1}")
                .retrieveFlux(String.class)
                .doOnSubscribe(s -> log.info("  [PRICE UPDATES] Subscription started"))
                .doOnNext(r -> log.info("  [PRICE UPDATES] {}", r))
                .doOnComplete(() -> log.info("  [PRICE UPDATES] Price updates stream completed"))
                .doOnError(e -> log.error("  [PRICE UPDATES] Error: {}", e.getMessage()))
                .onErrorResume(e -> {
                    log.error("  [PRICE UPDATES] Error: {}", e.getMessage());
                    return Flux.just("Error: " + e.getMessage());
                });

        log.info("  [PRICE UPDATES] Starting price update stream for good #1...");

        // =====================================================
        // 4. FIRE-AND-FORGET DEMO
        // =====================================================
        log.info("\n>>> DEMO 4: FIRE-AND-FORGET MODE");
        log.info("==================================");

        Mono<Void> demo4 = Flux.fromIterable(List.of(
                        "{\"eventType\":\"GOOD_CREATED\",\"eventData\":\"New product: Laptop - 25000 RUB\"}",
                        "{\"eventType\":\"GOOD_UPDATED\",\"eventData\":\"Updated price: Phone - 15000 RUB\"}",
                        "{\"eventType\":\"DARKSTORE_CREATED\",\"eventData\":\"New darkstore at Moscow\"}",
                        "{\"eventType\":\"DELIVERY_CREATED\",\"eventData\":\"New delivery scheduled\"}",
                        "{\"eventType\":\"GOOD_CREATED\",\"eventData\":\"New product: Tablet - 18000 RUB\"}"
                ))
                .doOnNext(event -> log.info("  [FIRE-AND-FORGET] Sending event: {}", event))
                .flatMap(event -> requester.route("event").data(event).send())
                .doOnComplete(() -> log.info("  [FIRE-AND-FORGET] All events sent (no response expected)"))
                .doOnError(e -> log.error("  [FIRE-AND-FORGET] Error: {}", e.getMessage()))
                .then();

        log.info("  [FIRE-AND-FORGET] Sending 5 events without waiting for response...");

        // =====================================================
        // 5. CHANNEL MODE (Bidirectional) DEMO
        // =====================================================
        log.info("\n>>> DEMO 5: CHANNEL MODE (BIDIRECTIONAL)");
        log.info("==========================================");

        Flux<String> demo5 = requester.route("channel")
                .data(Flux.fromIterable(List.of(
                                "Hello Server!",
                                "This is message #2",
                                "RSocket is awesome!",
                                "Testing bidirectional communication",
                                "Goodbye Server!"
                        ))
                        .doOnNext(msg -> log.info("  [CHANNEL] Sending to server: {}", msg)))
                .retrieveFlux(String.class)
                .doOnNext(r -> log.info("  [CHANNEL] Received from server: {}", r))
                .doOnComplete(() -> log.info("  [CHANNEL] Channel closed"))
                .doOnError(e -> log.error("  [CHANNEL] Error: {}", e.getMessage()));

        log.info("  [CHANNEL] Opening bidirectional channel...");

        // =====================================================
        // 6. REACTIVE PROCESSING DEMONSTRATION
        // =====================================================
        log.info("\n>>> DEMO 6: REACTIVE PROCESSING WITH REACTOR");
        log.info("==============================================");

        Flux<String> demo6 = requester.route("stream.goods")
                .data("{}")
                .retrieveFlux(String.class)
                // Filter: chỉ xử lý items hợp lệ
                .filter(data -> !data.contains("error") && !data.contains("Error"))
                // Map: transform data
                .map(data -> {
                    log.info("  [REACTIVE] Processing: {}", data);
                    return data.toUpperCase();
                })
                // Buffer: collect multiple items
                .buffer(3)
                .doOnNext(buffer -> log.info("  [REACTIVE] Buffer received {} items: {}", buffer.size(), buffer))
                .doOnSubscribe(s -> log.info("  [REACTIVE] Reactive stream subscription started"))
                .doOnComplete(() -> log.info("  [REACTIVE] Reactive processing completed!"))
                .flatMapIterable(buffer -> buffer);

        log.info("  [REACTIVE] Demonstrating reactive stream processing with buffer...");

        // =====================================================
        // SUBSCRIBE TO ALL DEMOS
        // =====================================================
        Flux.concat(
                        demo1,
                        demo1b,
                        demo1c,
                        demo1d,
                        Mono.fromRunnable(() -> log.info("  [REQUEST-RESPONSE] Demo 1 completed\n")),
                        demo2.then(),
                        demo3.then(),
                        demo4,
                        demo5.then(),
                        demo6.then()
                )
                .then()
                .doOnError(e -> log.error("RSocket Client Demo error: {}", e.getMessage(), e))
                .doOnSuccess(v -> log.info("RSocket Client Demo - All demos completed!"))
                .subscribe();

        // Keep the application running để nhận streams
        log.info("\n========================================");
        log.info("RSocket Client Demo - All demos started!");
        log.info("Waiting for streams to complete...");
        log.info("========================================");

        Thread.sleep(30000);
    }
}
