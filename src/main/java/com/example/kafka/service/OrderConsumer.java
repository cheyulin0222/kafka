package com.example.kafka.service;

import com.example.kafka.model.Order;
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

@Service
@Slf4j
@RequiredArgsConstructor
@RetryableTopic(
        attempts = "4",
        backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30000),
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
        exclude = {JsonProcessingException.class} // 格式錯誤不重試，直接進 DLT
)
public class OrderConsumer {

    private final ObjectMapper objectMapper;

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

    @DltHandler
    public void handleDlt(
            String message,
            // 將所有 Header 設為非必要 (required = false)，並用最寬鬆的 Object 接收
            @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) Object topic,
            @Header(name = "x-retry-topic-original-offset", required = false) Object offset
    ) {
        try {
            // 就算 topic 或 offset 是 null，Log 也不會崩潰
            log.error("=== 訊息宣告不治 ===");
            log.error("來自 Topic: {}", topic);
            log.error("原始 Offset: {}", offset);
            log.error("內容摘要: {}", message.substring(0, Math.min(message.length(), 100)));
        } catch (Exception e) {
            // 這是最後的保險絲，確保這一站一定會 Commit
            log.error("DLT 紀錄過程發生非預期錯誤");
        }
    }


//    @KafkaHandler
//    public void handleOrder(Order order) {
//        log.info("收到訂單! ID = {}, 商品 = {}, 金額 = {}", order.getOrderId(), order.getProduct(), order.getAmount());
//    }
//
//    @KafkaHandler
//    public void handleUser(User user) {
//        log.info("收到使用者! ID = {}, 名稱 = {}", user.getId(), user.getName());
//    }


//    @KafkaListener(
//            topics = "orders",
//            groupId = "my-order-group",
//            concurrency = "3"
//    )
//    public void listen(ConsumerRecord<String, String> record) {
//        System.out.printf("收到訂單！Key: %s, Value: %s, 分區: %d, Offset: %d%n",
//                record.key(), record.value(), record.partition(), record.offset());
//    }

}
