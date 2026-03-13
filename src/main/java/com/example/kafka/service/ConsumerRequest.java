package com.example.kafka.service;

import com.example.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerRequest {

    // 搭配
    // spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
    // spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
    @KafkaListener(
            topics = "orders",
            groupId = "my-order-group",
            concurrency = "3"
    )
    public void listener(ConsumerRecord<String, String> record) {
        log.info("收到訂單! Key = {}, Value = {}, partition = {}, Offset = {}",
                record.key(), record.value(), record.partition(), record.offset());
    }

    // 直接收 value
    @KafkaListener(
            topics = "orders",
            groupId = "my-order-group",
            concurrency = "3"
    )
    public void listener(String value) {
        log.info("收到訂單! Value = {}", value);
    }


    // 搭配
    // spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
    // #spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
    @KafkaListener(
            topics = "orders",
            groupId = "my-order-group",
            concurrency = "3"
    )
    public void objectListener(ConsumerRecord<String, Order> record) {
        log.info("收到訂單! Key = {}, Value = {}, partition = {}, Offset = {}",
                record.key(), record.value(), record.partition(), record.offset());
    }

    // 直接收 value
    @KafkaListener(
            topics = "orders",
            groupId = "my-order-group",
            concurrency = "3"
    )
    public void listener(Order value) {
        log.info("收到訂單! Value = {}", value);
    }

    // 指定欄位
    @KafkaListener(
            topics = "orders",
            groupId = "my-order-group",
            concurrency = "3"
    )
    public void listener(Order value, @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) Object topic) {
        log.error("來自 Topic: {}", topic);
        log.info("收到訂單! Value = {}", value);
    }

}
