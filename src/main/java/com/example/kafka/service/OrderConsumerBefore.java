package com.example.kafka.service;

import com.example.kafka.model.Order;
import com.example.kafka.model.OrderEntity;
import com.example.kafka.model.ProcessMessage;
import com.example.kafka.repository.OrderRepository;
import com.example.kafka.repository.ProcessedMessageRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
@RetryableTopic(
        attempts = "4",
        backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30000),
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
        exclude = {JsonProcessingException.class} // 格式錯誤不重試，直接進 DLT
)
public class OrderConsumerBefore {

    private final ObjectMapper objectMapper;
    private final OrderRepository orderRepository;
    private final ProcessedMessageRepository processedMessageRepository;


    // Producer 端：用 @Transactional 保證「訂單」與「Outbox」同步存入。
    // Relay 端：用 @Scheduled 保證訊息「至少發出一次」。
    // Consumer 端：用「去重表」保證「即便收到兩次，也只執行一次」。
    @KafkaListener(topics = "order_topic", groupId = "order_group")
    @Transactional
    public void consume(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String message = record.value();
        Order order = objectMapper.readValue(message, Order.class);
        String orderId = order.getOrderId();

        if (processedMessageRepository.existsById(orderId)) {
            log.warn("訊息已處理過，跳過: {}", orderId);
            return;
        }

        OrderEntity orderEntity = OrderEntity.builder()
                .orderId(orderId)
                .product(order.getProduct())
                .amount(order.getAmount())
                .build();

        orderRepository.save(orderEntity);

        processedMessageRepository.save(new ProcessMessage(orderId));
    }

//    @KafkaHandler
    @KafkaListener(
            topics = "orders",
            groupId = "my-order-group-fixed",
            concurrency = "3"
    )
    public void handle(ConsumerRecord<String, String> record,
                       @Header(value = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer attempt) throws JsonProcessingException {
        // 預設第一次進來 attempt 可能是 null，給個預設值 1
        int currentAttempt = (attempt == null) ? 1 : attempt;
        log.info("【消費開始】嘗試次數: {}, Partition: {}, Offset: {}, Key: {}",
                currentAttempt, record.partition(), record.offset(), record.key());


        String message = record.value();

        // 解析 JSON：若失敗會拋出 JsonProcessingException，因被 exclude 會直接送往 DLT
        Order order = objectMapper.readValue(message, Order.class);

        log.info("成功解析訂單! ID = {}, 商品 = {}", order.getOrderId(), order.getProduct());

        // 模擬業務邏輯報錯：會觸發重試 (進入 orders-retry)
        if ("error".equals(order.getProduct())) {
            log.warn("模擬業務異常，觸發重試...");
            throw new RuntimeException("資料庫連線失敗");
        }
    }


}
