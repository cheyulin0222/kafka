package com.example.kafka.service;
//
//import com.example.kafka.model.OutboxEntity;
//import com.example.kafka.repository.OutboxRepository;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
//@Component
//@Slf4j
//@RequiredArgsConstructor
//public class OutboxRelay {
//
//    private final OutboxRepository outboxRepository;
//    private final KafkaTemplate<String, String> kafkaTemplate;
//
//    @Scheduled(fixedDelay = 1000)
//    public void relayMessage() {
//
//        List<OutboxEntity> pendingMessages = outboxRepository.findByStatus("PENDING");
//
//        for (OutboxEntity message : pendingMessages) {
//            try {
//                // 2. 發送 Kafka (這一步是外部 IO，不建議放在 DB 事務內)
//                kafkaTemplate.send(message.getTopic(), message.getPayload()).get(5, TimeUnit.SECONDS);
//
//                // 3. 成功後，單獨更新這一筆的狀態
//                updateStatus(message.getId(), "SENT");
//
//                log.info("成功轉發 Outbox 訊息 ID: {}", message.getId());
//            } catch (Exception e) {
//                log.error("訊息 ID {} 轉發失敗: {}", message.getId(), e.getMessage());            }
//        }
//
//    }
//
//    @Transactional // 只有更新狀態需要事務
//    public void updateStatus(Integer id, String status) {
//        outboxRepository.updateStatusById(id, status);
//    }
//
//
//}
