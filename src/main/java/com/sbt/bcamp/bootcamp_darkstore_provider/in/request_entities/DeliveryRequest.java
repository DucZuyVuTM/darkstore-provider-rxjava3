package com.sbt.bcamp.bootcamp_darkstore_provider.in.request_entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sbt.bcamp.bootcamp_darkstore_provider.Status;

import java.time.LocalDateTime;

public record DeliveryRequest(
        @JsonIgnore int id,
        int darkstoreId,
        LocalDateTime deliveryDateTime,
        Status status
) {}
