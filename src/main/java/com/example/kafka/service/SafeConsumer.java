package com.example.kafka.service;

import com.example.kafka.model.Order;
import com.example.kafka.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KafkaListener(
        topics = "order-topic",
        // 不直接使用 JsonDeserializer
        // 把它包在 ErrorHandlingDeserializer
        // 當 Json 壞掉時，它會抓到錯誤，並傳遞一個特殊的「失敗標記」給下游
        // 不是直接噴 Exception
        containerFactory = "kafkaListenerContainerFactory"
)
public class SafeConsumer {

    // 專門處理 Order
    @KafkaHandler
    public void handleOrder(Order order) {
        log.info("成功處理正常訂單: {}", order);
    }

    // 專門處理 User
    @KafkaHandler
    public void handleUser(User user) {
        log.info("成功處理正常使用者: {}", user);
    }

    // 無法辨別，叫給標註 isDefault = true 的
    @KafkaHandler(isDefault = true)
    public void handleDefault(Object data) {
        // 如果是因為反序列化失敗進來的，data 可能會是 null 或是 DeserializationException 物件
        log.error("=== 攔截到錯誤格式或未知訊息 ===");
        log.error("原始資料內容: {}", data);
    }
}
