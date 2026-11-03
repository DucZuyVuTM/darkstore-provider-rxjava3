package com.sbt.bcamp.bootcamp_darkstore_provider.reactive.runner;

import com.sbt.bcamp.bootcamp_darkstore_provider.reactive.service.ReactiveGoodService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ReactiveDataRunner implements CommandLineRunner {

    private final ReactiveGoodService reactiveGoodService;

    @Override
    public void run(String... args) throws Exception {
        // Begin
        reactiveGoodService.processGoodsStream();
        
        // Wait 3s for RxJava to finish before Spring Boot is off
        Thread.sleep(3000); 
    }
}
