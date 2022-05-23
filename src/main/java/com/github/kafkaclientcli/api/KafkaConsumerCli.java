package com.github.kafkaclientcli.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.kafkaclientcli.util.ConsumerUtils;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
@CommandLine.Command(name = "consume", mixinStandardHelpOptions = true)
public class KafkaConsumerCli implements Runnable {
    private final ObjectMapper mapper = new ObjectMapper();
    @Autowired
    private ConsumerFactory<?, ?> consumerFactory;
    @CommandLine.Option(names = {"-t", "--topic"}, required = true, description = "topic from consume")
    private String topic;
    @CommandLine.Option(names = {"-l", "--last"}, description = "flag for consuming last")
    private boolean last;
    @CommandLine.Option(names = {"-g", "--group-id"}, required = true, description = "group_id")
    private String groupId;
    @CommandLine.Option(names = {"-n", "--number"}, description = "number of last messages")
    private Long lastNumberMessages;
    @CommandLine.Option(names = {"--timeout"}, defaultValue = "1000")
    private Long timeOut;
    @CommandLine.Option(names = {"-p", "--partition"}, defaultValue = "0")
    private int partition;


    @SneakyThrows
    @Override
    public void run() {
        var consumer = consumerFactory.createConsumer(groupId, groupId);
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        TopicPartition topicPart = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPart));
        try {
            if (last) {
                ConsumerUtils.getLast(consumer, topicPart, timeOut, mapper);
            } else {
                ConsumerUtils.consumeFromTo(consumer, topicPart, lastNumberMessages, mapper, timeOut);
            }
        } finally {
            consumer.close();
        }
    }
}
