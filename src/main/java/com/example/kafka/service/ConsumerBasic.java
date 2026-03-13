package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerBasic {

    @KafkaListener(
            topics = "orders",
            groupId = "my-order-group",
            concurrency = "3")
    public void listen(ConsumerRecord<String, String> record) {
        log.info("收到訂單! Key = {}, Value = {}, partition = {}, Offset = {}",
                record.key(), record.value(), record.partition(), record.offset());
    }
}
