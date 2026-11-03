package com.sbt.bcamp.bootcamp_darkstore_provider.reactive.service;

import com.sbt.bcamp.bootcamp_darkstore_provider.entities.Good;
import com.sbt.bcamp.bootcamp_darkstore_provider.repository.GoodRepository;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ReactiveGoodService {

    // Reuse repository
    private final GoodRepository goodRepository;

    public void processGoodsStream() {
        System.out.println("--- Begin to process Postgres data ---");

        // Fetch data from the database asynchronously (on an I/O thread)
        Flowable<Iterable<Good>> dbData = Flowable.fromCallable(() -> goodRepository.findAll())
                .subscribeOn(Schedulers.io());

        // Process data
        dbData
            // Break down a `Iterable<Good>` into individual `Good` objects.
            .flatMapIterable(goods -> goods)
            .filter(good -> good.getId() > 0)
            
            // Convert to readable string
            .map(good -> "Product: " + good.getName() + " | Price: " + good.getPrice())
            
            // Go to computation
            .observeOn(Schedulers.computation())
            
            // Subscribe to get result and print
            .subscribe(
                    result -> System.out.println(">> " + result),
                    error -> System.err.println("Lỗi luồng: " + error.getMessage()),
                    () -> System.out.println("--- End processing ---")
            );
    }
}
