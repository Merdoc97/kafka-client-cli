package com.github.kafkaclientcli.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@UtilityClass
public class Aggregator {


    public Collection<CompletableFuture<?>> invokeAllAndReturn(Collection<CompletableFuture<?>> futures, boolean waitForAllDone) {
        var futuresArrays = new CompletableFuture[futures.size()];
        futures.toArray(futuresArrays);
        var result = CompletableFuture.allOf(futuresArrays);

        if (waitForAllDone) {
            while (!result.isDone()) {
                try {
                    log.info("{} working...", Thread.currentThread().getName());
                    TimeUnit.SECONDS.sleep(1);

                } catch (InterruptedException e) {
                    log.warn("Exception occurred in {} class ", Aggregator.class.getCanonicalName(), e);
                    throw new RuntimeException("Exception occurred in " + Aggregator.class.getCanonicalName() + " class while execution");
                }
            }
            return futures;
        }
        return futures;
    }
}
