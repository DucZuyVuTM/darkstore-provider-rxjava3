package com.sbt.bcamp.bootcamp_darkstore_provider.service;

import com.sbt.bcamp.bootcamp_darkstore_provider.entities.Darkstore;
import com.sbt.bcamp.bootcamp_darkstore_provider.entities.Delivery;
import com.sbt.bcamp.bootcamp_darkstore_provider.entities.Good;
import com.sbt.bcamp.bootcamp_darkstore_provider.exceptions.SQL.RecordNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class RSocketService {

    private final GoodService goodService;
    private final DarkstoreService darkstoreService;
    private final DeliveryService deliveryService;

    private final Map<String, List<String>> channelMessages = new ConcurrentHashMap<>();
    private final AtomicInteger channelIdCounter = new AtomicInteger(0);

    // ==================== Request-Response ====================

    public Mono<Good> getGoodById(int id) {
        log.info("RSocket Request-Response: Getting good by ID: {}", id);
        return Mono.fromCallable(() -> goodService.getGoodById(id))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(RecordNotFoundException.class, e ->
                        log.warn("RSocket: Good not found with ID: {}", id));
    }

    public Mono<Darkstore> getDarkstoreById(int id) {
        log.info("RSocket Request-Response: Getting darkstore by ID: {}", id);
        return Mono.fromCallable(() -> darkstoreService.getDarkstoreById(id))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(RecordNotFoundException.class, e ->
                        log.warn("RSocket: Darkstore not found with ID: {}", id));
    }

    public Mono<Delivery> getDeliveryById(int id) {
        log.info("RSocket Request-Response: Getting delivery by ID: {}", id);
        return Mono.fromCallable(() -> deliveryService.getDeliveryById(id))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(RecordNotFoundException.class, e ->
                        log.warn("RSocket: Delivery not found with ID: {}", id));
    }

    // ==================== Request-Stream ====================

    /**
     * Stream danh sách goods, mỗi phần tử cách nhau 500ms.
     * Nếu không có goods, trả về Flux rỗng (client sẽ thấy stream hoàn tất ngay).
     */
    public Flux<Good> streamGoodsWithDelay() {
        log.info("RSocket Request-Stream: Streaming goods with delay");

        return Mono.fromCallable(() -> {
                    Iterable<Good> iterable = goodService.getAllGoods();
                    List<Good> list = new ArrayList<>();
                    iterable.forEach(list::add);
                    return list;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(goodsList -> {
                    if (goodsList.isEmpty()) {
                        log.info("RSocket Stream: No goods available");
                        return Flux.empty();
                    }
                    return Flux.interval(Duration.ofMillis(500))
                            .take(goodsList.size())
                            .map(i -> {
                                Good good = goodsList.get(i.intValue());
                                log.info("RSocket Stream: Sending good #{} - {} - {} RUB",
                                        i + 1, good.getName(), good.getPrice());
                                return good;
                            });
                });
    }

    /**
     * Stream cập nhật giá mô phỏng, 5 lần, mỗi lần cách 1s.
     * Nếu good không tồn tại, trả về một phần tử thông báo lỗi trong stream.
     */
    public Flux<String> streamPriceUpdates(int goodId) {
        log.info("RSocket Request-Stream: Streaming price updates for good ID: {}", goodId);

        return Mono.fromCallable(() -> goodService.getGoodById(goodId))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(good -> {
                    log.info("RSocket Stream: Starting price updates for: {} (Base price: {} RUB)",
                            good.getName(), good.getPrice());
                    return Flux.interval(Duration.ofSeconds(1))
                            .take(5)
                            .map(i -> {
                                double variation = (Math.random() - 0.5) * 10000;
                                double newPrice = good.getPrice() + variation;
                                String message = String.format(
                                        "Good #%d (%s) - Updated price: %.2f RUB [Update #%d]",
                                        goodId, good.getName(), newPrice, i + 1);
                                log.info("RSocket Stream: {}", message);
                                return message;
                            });
                })
                .onErrorResume(RecordNotFoundException.class, e -> {
                    log.error("RSocket Stream: Good not found with ID: {}", goodId);
                    return Flux.just("{\"error\":\"RECORD_NOT_FOUND\",\"message\":\"Good not found with ID: " + goodId + "\"}");
                });
    }

    // ==================== Fire-and-Forget ====================

    public Mono<Void> receiveEvent(String eventType, String eventData) {
        log.info("RSocket Fire-and-Forget: Received event - Type: {}, Data: {}", eventType, eventData);

        return Mono.fromRunnable(() -> {
            String normalized = eventType == null ? "" : eventType.toUpperCase();
            switch (normalized) {
                case "GOOD_CREATED" -> log.info("  -> Processing GOOD_CREATED event: {}", eventData);
                case "GOOD_UPDATED" -> log.info("  -> Processing GOOD_UPDATED event: {}", eventData);
                case "DARKSTORE_CREATED" -> log.info("  -> Processing DARKSTORE_CREATED event: {}", eventData);
                case "DELIVERY_CREATED" -> log.info("  -> Processing DELIVERY_CREATED event: {}", eventData);
                default -> log.info("  -> Processing generic event: {} - {}", eventType, eventData);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    // ==================== Channel ====================

    /**
     * Channel hai chiều: nhận stream từ client, echo lại cho client.
     * Mỗi message được đánh số thứ tự tăng dần, bắt đầu từ 1.
     */
    public Flux<String> handleChannel(Flux<String> clientStream) {
        log.info("RSocket Channel: Channel opened, waiting for client messages...");

        String channelId = "channel_" + channelIdCounter.incrementAndGet();
        AtomicInteger counter = new AtomicInteger(0);

        return clientStream
                .doOnSubscribe(s -> log.info("RSocket Channel[{}]: Subscription started", channelId))
                .doOnNext(message -> {
                    int n = counter.incrementAndGet();
                    log.info("RSocket Channel[{}]: Received message #{}: {}", channelId, n, message);
                    channelMessages.computeIfAbsent(channelId, k -> new ArrayList<>()).add(message);
                })
                .doOnError(e -> log.error("RSocket Channel[{}]: Error", channelId, e))
                .doOnComplete(() -> log.info("RSocket Channel[{}]: Client stream completed, total: {}",
                        channelId, counter.get()))
                .map(message -> {
                    int n = counter.get();
                    String response = String.format("[Server #%d] Echo: %s", n, message);
                    log.info("RSocket Channel[{}]: Sending response: {}", channelId, response);
                    return response;
                })
                .doOnComplete(() -> log.info("RSocket Channel[{}]: Channel closing", channelId));
    }

    public Mono<List<String>> getChannelMessages(String channelId) {
        return Mono.fromSupplier(() ->
                channelMessages.getOrDefault(channelId, List.of()));
    }
}
