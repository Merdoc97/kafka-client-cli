package com.github.kafkaclientcli.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.kafkaclientcli.util.Aggregator;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Component
@RequiredArgsConstructor
@CommandLine.Command(name = "produce-performance", mixinStandardHelpOptions = true)
public class KafkaProducerMultiThreadedCli implements Runnable {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @CommandLine.Option(names = {"-f", "--file"}, required = true, description = "link to file with body in json format")
    private File[] inputFiles;

    @CommandLine.Option(names = {"-h", "--headers"}, required = false, description = "headers in key:value format multiple headers via -h")
    private String[] headers;

    @CommandLine.Option(names = {"-t", "--topic"}, required = true, description = "topic name")
    private String topic;

    @CommandLine.Option(names = {"-k", "--key"}, required = false, description = "key for kafka template")
    private String key;

    @CommandLine.Option(names = {"-r", "--random"}, required = false, description = "flag for reading files randomly")
    private boolean random;

    @CommandLine.Option(names = {"-n", "--threads"}, required = true, description = "number of messages")
    private Integer numberOfThreads;

    @CommandLine.Option(names = {"-m", "--messages"}, required = true, description = "number of threads")
    private Integer numberOfMessages;

    private final Random randomGenerator = new Random();

    @Override
    public void run() {
        var executor = Executors.newFixedThreadPool(numberOfThreads);
        var messagesPerFile = numberOfMessages / inputFiles.length;
        Collection<CompletableFuture<?>> futureList = IntStream.iterate(0, i -> i + 1)
                .limit(inputFiles.length)
                .mapToObj(o -> {
                    if (random) {
                        return generateFutures(inputFiles[randomGenerator.nextInt(inputFiles.length) - 0], messagesPerFile, executor);
                    } else
                        return generateFutures(inputFiles[o], messagesPerFile, executor);
                })
                .flatMap(Collection::stream)
                .collect(toShuffledList());
        log.info("generates messages {} x {}={}", messagesPerFile, inputFiles.length, messagesPerFile * inputFiles.length);
        Aggregator.invokeAllAndReturn(futureList, true);
    }

    private static final Collector<?, ?, ?> SHUFFLER = Collectors.collectingAndThen(
            Collectors.toCollection(ArrayList::new),
            list -> {
                Collections.shuffle(list);
                return list;
            }
    );

    @SuppressWarnings("unchecked")
    public static <T> Collector<T, ?, List<T>> toShuffledList() {
        return (Collector<T, ?, List<T>>) SHUFFLER;
    }
    private List<CompletableFuture<?>> generateFutures(File file, int numberOfMessages, ExecutorService executor) {
        return IntStream.iterate(0, i -> i + 1)
                .limit(numberOfMessages)
                .mapToObj(number -> generateFuture(executor, file))
                .collect(Collectors.toList());
    }

    private CompletableFuture<?> generateFuture(ExecutorService executorService, File inputFile) {
        return CompletableFuture.supplyAsync(() -> {
            var producerRecord = new ProducerRecord<String, Object>(topic, key, validateFormat(inputFile));
            addHeaders(producerRecord.headers());
            kafkaTemplate.send(producerRecord);
            log.info("Thread {} working...", Thread.currentThread().getName());
            return "done";
        }, executorService);
    }

    @SneakyThrows
    private Map<String, String> validateFormat(File inputFile) {
        return mapper.readValue(inputFile, Map.class);
    }

    private void addHeaders(Headers headers) {
        generateHeaders().forEach((s, s2) -> headers.add(s, s2.getBytes(StandardCharsets.UTF_8)));
    }

    private Map<String, String> generateHeaders() {
        if (headers != null) {
            return Arrays.stream(headers)
                    .collect(Collectors.toMap(s -> s.split(":")[0], s -> s.split(":")[1]));
        }
        return Map.of();
    }
}
