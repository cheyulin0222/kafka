package com.example.kafka.service;

import com.example.kafka.model.Order;
import com.example.kafka.model.OrderEntity;
import com.example.kafka.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // 有 Transactional，進入方法，會先向 Broker 獲取 PID
    @Transactional
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


    @Transactional
    public void createOrderWithDelay(String orderId, String product, double amount) throws JsonProcessingException, InterruptedException {
        log.info("【Producer A】開始事務，ID: {}", orderId);

        Order order1 = Order.builder()
                .orderId("first message")
                .product(product)
                .amount(amount)
                .build();

        String json1 = objectMapper.writeValueAsString(order1);

        kafkaTemplate.send("orders", orderId, json1);

        log.info("【Producer A】進入休眠 20 秒，請在此時啟動/呼叫 Producer B...");
        Thread.sleep(20000); // 讓我們有時間去觸發另一個 Producer

        Order order2 = Order.builder()
                .orderId("second message")
                .product(product)
                .amount(amount)
                .build();

        String json2 = objectMapper.writeValueAsString(order2);


        log.info("【Producer A】睡醒了，嘗試發送最後訊息並提交...");
        kafkaTemplate.send("orders", orderId, json2);

        log.info("【Producer A】嘗試提交事務...");
    }

    // 現在這個註解會啟動 JpaTransactionManager
    // 並且因為我們設定了連動，它也會同時開啟 Kafka 事務
    @Transactional
    public void createOrderWithSync(String orderId, String product, double amount) {
        log.info("--- 執行全鏈路事務: {} ---", orderId);

        OrderEntity order = new OrderEntity(orderId, product, amount);
        orderRepository.save(order);
        log.info("1. 第一筆 JPA 已調用 save");

        kafkaTemplate.send("notifications", orderId, "Order Created");
        log.info("2. Kafka 已調用 send");

        // 3. 【強迫失敗】與其等 JPA 自動判斷，我們直接手動拋出異常
        // 或者你可以存入一個欄位長度超過資料庫限制的字串，讓 MySQL 報錯
        if (orderId.equals("100")) {
            log.error("!!! 模擬發生嚴重錯誤，觸發連坐回滾 !!!");
            throw new RuntimeException("DB 連線中斷或觸發約束衝突");
        }

        log.info("3. 這一行理論上不應該被印出，因為上面應該噴 Duplicate Key 錯誤");

    }

    @Transactional
    public void createOrderOutbox(String orderId, String product, double amount) {
        log.info("--- 執行全鏈路事務: {} ---", orderId);

        OrderEntity order = new OrderEntity(orderId, product, amount);
        orderRepository.save(order);
        log.info("1. 第一筆 JPA 已調用 save");

        // 2. 存發件箱（這取代了原本的 kafkaTemplate.send）
        OutboxEntity outbox = new OutboxEntity();
        outbox.setTopic("order_topic");
        outbox.setPayload("{ \"orderId\": \"" + orderId + "\" }");
        outbox.setStatus("PENDING");
        outboxRepository.save(outbox);
    }
}
