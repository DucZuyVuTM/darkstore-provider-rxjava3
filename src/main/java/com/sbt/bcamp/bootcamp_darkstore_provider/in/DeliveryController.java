package com.sbt.bcamp.bootcamp_darkstore_provider.in;

import com.sbt.bcamp.bootcamp_darkstore_provider.entities.Delivery;
import com.sbt.bcamp.bootcamp_darkstore_provider.in.request_entities.DeliveryRequest;
import com.sbt.bcamp.bootcamp_darkstore_provider.service.DeliveryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin
@RestController
public class DeliveryController {
    @Autowired
    private DeliveryService service;

    @GetMapping("/order")
    public ResponseEntity<Iterable<Delivery>> getAllDeliveries() {
        return ResponseEntity.ok(service.getAllDeliveries());
    }

    @GetMapping("/order/{id}")
    public ResponseEntity<Delivery> getDeliveryById(@PathVariable int id) {
        return ResponseEntity.ok(service.getDeliveryById(id));
    }

    @PostMapping("/order")
    public ResponseEntity<Void> createDelivery(@RequestBody DeliveryRequest delivery) {
        service.createDelivery(delivery);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @PutMapping("/order")
    public ResponseEntity<Void> updateDelivery(@RequestBody DeliveryRequest delivery) {
        service.updateDelivery(delivery);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping("/order/{id}")
    public ResponseEntity<Void> deleteDelivery(@PathVariable int id) {
        service.deleteDelivery(id);
        return ResponseEntity.ok().build();
    }
}
