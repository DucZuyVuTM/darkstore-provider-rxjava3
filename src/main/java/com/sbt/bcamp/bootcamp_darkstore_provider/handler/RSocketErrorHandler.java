package com.sbt.bcamp.bootcamp_darkstore_provider.handler;

import com.sbt.bcamp.bootcamp_darkstore_provider.exceptions.SQL.RecordNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Mono;

import java.util.Map;

@Controller
@Slf4j
public class RSocketErrorHandler {

    /**
     * RecordNotFoundException - Return error info to client
     */
    @MessageExceptionHandler(RecordNotFoundException.class)
    public Mono<Map<String, Object>> handleRecordNotFoundException(RecordNotFoundException ex) {
        log.error("RSocket Error - Record not found: {}", ex.getMessage());
        return Mono.just(Map.of(
                "error", "RECORD_NOT_FOUND",
                "message", ex.getMessage()
        ));
    }

    /**
     * Generic Error Handler
     */
    @MessageExceptionHandler(Exception.class)
    public Mono<Map<String, Object>> handleGeneralException(Exception ex) {
        log.error("RSocket Error - Unexpected error: {}", ex.getMessage(), ex);
        return Mono.just(Map.of(
                "error", "INTERNAL_ERROR",
                "message", ex.getMessage() != null ? ex.getMessage() : "Unknown error"
        ));
    }
}
