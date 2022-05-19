package com.github.kafkaclientcli.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@UtilityClass
@Slf4j
public class ConsumerUtils {

    @SneakyThrows
    public void getLast(Consumer consumer, TopicPartition topicPart, Long timeout, ObjectMapper mapper) {
        consumer.seekToEnd(Collections.singletonList(topicPart));
        if (consumer.position(topicPart) > 0L) {
            consumer.seek(topicPart, consumer.position(topicPart) - 1L);
        }
        ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(timeout));
        ConsumerRecord<?, ?> record = records.iterator().next();
        if (record != null) {
            consumer.commitSync();
            log.info("-----------body-------------");
            log.info(record.value().toString());
            log.info("-----------headers-------------");
            log.info(mapper.writeValueAsString(convertHeaders(record.headers())));
        }
    }

    @SneakyThrows
    public void consumeFromTo(Consumer<?, ?> consumer, TopicPartition topicPart,
                              Long from, Long to, ObjectMapper mapper, long timeoutMs) {
        if (from == null || to == null) {
            log.error("from and to is required if last not set");
            System.exit(-1);
        }

        consumer.seek(topicPart, from);
        var records = consumer.poll(Duration.ofMillis(timeoutMs));
        var iterator = records.iterator();
        consumer.commitAsync();

        int counter = 0;
        do {
            log.info("----------------begin-------------------");
            var record = iterator.next();
            log.info("-----------body-------------");
            log.info(record.value().toString());
            log.info("-----------headers-------------");
            log.info(mapper.writeValueAsString(convertHeaders(record.headers())));
            ++counter;
            log.info("----------------end----------------------");
        }
        while (iterator.hasNext() && counter != (1 + from - to));
    }

    private Map<String, String> convertHeaders(Headers headers) {
        Map<String, String> map = new HashMap<>();
        var iterator = headers.iterator();
        while (iterator.hasNext()) {
            var record = (RecordHeader) iterator.next();
            map.put(record.key(), new String(record.value()));
        }
        return map;
    }

}
