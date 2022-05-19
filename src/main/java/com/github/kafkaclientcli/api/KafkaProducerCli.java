package com.github.kafkaclientcli.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
@CommandLine.Command(name = "produce", mixinStandardHelpOptions = true)
public class KafkaProducerCli implements Runnable {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @CommandLine.Option(names = {"-f", "--file"}, required = true, description = "link to file with body in json format")
    private File inputFile;

    @CommandLine.Option(names = {"-h", "--headers"}, required = false, description = "headers in key:value format multiple headers via -h")
    private String[] headers;

    @CommandLine.Option(names = {"-t", "--topic"}, required = true, description = "topic name")
    private String topic;

    @Override
    public void run() {
        var producerRecord = new ProducerRecord<String, Object>(topic, validateFormat());
        addHeaders(producerRecord.headers());
        kafkaTemplate.send(producerRecord);
    }

    @SneakyThrows
    private Map<String, String> validateFormat() {
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
