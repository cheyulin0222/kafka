package com.example.kafka.service;

import com.example.kafka.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // 有 Transactional，進入方法，會先向 Broker 獲取 PID
//    @Transactional
    public void createOrder(String orderId, String product, double amount) throws JsonProcessingException {
        log.info("--- 開始執行訂單事務: {} ---", orderId);

        // 1. 模擬資料庫操作
        log.info("1. 正在儲存訂單到資料庫...");

        Order order = Order.builder()
                .orderId(orderId)
                .product(product)
                .amount(amount)
                .build();

        String jsonString = objectMapper.writeValueAsString(order);

        // 2. 發送 Kafka 訊息
        log.info("2. 正在發送 Kafka 訊息...");

        kafkaTemplate.send("orders", orderId, jsonString);

        if ("fail".equalsIgnoreCase(product)) {
            log.error("!!! 發生異常，觸發事務回滾 !!!");
            throw new RuntimeException("資料庫寫入失敗或連線中斷");
        }

        log.info("--- 事務提交完畢 ---");
    }
}
