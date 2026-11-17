package com.sbt.bcamp.bootcamp_darkstore_provider.handler;

import com.sbt.bcamp.bootcamp_darkstore_provider.service.RSocketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Controller
@RequiredArgsConstructor
@Slf4j
public class RSocketHandler {

    private final RSocketService rSocketService;

    // =====================================================
    // REQUEST-RESPONSE MODELS
    // Action: request.good, request.darkstore, request.delivery
    // =====================================================

    /**
     * Request-Response: Get good by ID
     * Client sends: {"id": 1}
     * Action: "request.good"
     */
    @MessageMapping("request.good")
    public Mono<com.sbt.bcamp.bootcamp_darkstore_provider.entities.Good> getGood(
            com.sbt.bcamp.bootcamp_darkstore_provider.entities.Good request) {
        return rSocketService.getGoodById(request.getId());
    }

    /**
     * Request-Response: Get darkstore by ID
     * Client sends: {"id": 1}
     * Action: "request.darkstore"
     */
    @MessageMapping("request.darkstore")
    public Mono<com.sbt.bcamp.bootcamp_darkstore_provider.entities.Darkstore> getDarkstore(
            com.sbt.bcamp.bootcamp_darkstore_provider.entities.Darkstore request) {
        return rSocketService.getDarkstoreById(request.getId());
    }

    /**
     * Request-Response: Get delivery by ID
     * Client sends: {"id": 1}
     * Action: "request.delivery"
     */
    @MessageMapping("request.delivery")
    public Mono<com.sbt.bcamp.bootcamp_darkstore_provider.entities.Delivery> getDelivery(
            com.sbt.bcamp.bootcamp_darkstore_provider.entities.Delivery request) {
        return rSocketService.getDeliveryById(request.getId());
    }

    // =====================================================
    // REQUEST-STREAM MODELS
    // Action: stream.goods, stream.price.updates
    // =====================================================

    /**
     * Request-Stream: Stream all goods with delay
     * Action: "stream.goods"
     * Returns: List of goods with 500ms delay between each
     */
    @MessageMapping("stream.goods")
    @SendTo("response.stream.goods")
    public Flux<com.sbt.bcamp.bootcamp_darkstore_provider.entities.Good> streamGoods() {
        return rSocketService.streamGoodsWithDelay();
    }

    /**
     * Request-Stream: Stream price updates for a specific good
     * Client sends: {"id": 1} (good ID)
     * Action: "stream.price.updates"
     * Returns: 5 price update messages with 1s delay
     */
    @MessageMapping("stream.price.updates")
    @SendTo("response.stream.price.updates")
    public Flux<String> streamPriceUpdates(
            com.sbt.bcamp.bootcamp_darkstore_provider.entities.Good request) {
        return rSocketService.streamPriceUpdates(request.getId());
    }

    // =====================================================
    // FIRE-AND-FORGET MODEL
    // Action: event.{event_type}
    // =====================================================

    /**
     * Fire-and-Forget: Receive events without response
     * Client sends: {"eventType": "GOOD_CREATED", "eventData": "..."}
     * Action: "event"
     * No response sent back
     */
    @MessageMapping("event")
    public Mono<Void> receiveEvent(
            RSocketEventRequest request) {
        log.info("RSocket Event received: {} - {}", request.getEventType(), request.getEventData());
        return rSocketService.receiveEvent(request.getEventType(), request.getEventData());
    }

    // =====================================================
    // CHANNEL MODEL (Bidirectional)
    // Action: channel
    // =====================================================

    /**
     * Channel: Bidirectional communication
     * Client sends: stream of messages
     * Server responds: stream of echoed messages
     * Action: "channel"
     */
    @MessageMapping("channel")
    @SendTo("response.channel")
    public Flux<String> handleChannel(Flux<String> clientMessages) {
        return rSocketService.handleChannel(clientMessages);
    }

    /**
     * Request-Response: Get channel messages
     * Client sends: {"channelId": "channel_0"}
     * Action: "request.channel.messages"
     */
    @MessageMapping("request.channel.messages")
    public Mono<List<String>> getChannelMessages(
            RSocketChannelRequest request) {
        return rSocketService.getChannelMessages(request.getChannelId());
    }

    // =====================================================
    // INNER CLASSES FOR RSOCKET EVENTS
    // =====================================================

    public static class RSocketEventRequest {
        private String eventType;
        private String eventData;

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public String getEventData() {
            return eventData;
        }

        public void setEventData(String eventData) {
            this.eventData = eventData;
        }
    }

    public static class RSocketChannelRequest {
        private String channelId;

        public String getChannelId() {
            return channelId;
        }

        public void setChannelId(String channelId) {
            this.channelId = channelId;
        }
    }
}
