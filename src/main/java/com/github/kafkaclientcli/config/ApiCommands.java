package com.github.kafkaclientcli.config;

import com.github.kafkaclientcli.api.KafkaConsumerCli;
import com.github.kafkaclientcli.api.KafkaProducerCli;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

@Slf4j
@Getter
@Component
@RequiredArgsConstructor
@CommandLine.Command(name = "api", mixinStandardHelpOptions = true, description = "Kafka console commands",
        version = "0.1.0", synopsisSubcommandLabel = "COMMAND",
        subcommands = {
                KafkaProducerCli.class,
                KafkaConsumerCli.class
        })
public class ApiCommands {

}