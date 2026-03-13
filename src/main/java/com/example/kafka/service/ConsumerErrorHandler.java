package com.example.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
@RetryableTopic(
        attempts = "4", // 包含原始嘗試，共 4 次
        // 第一次等 2s ， 之後每次 *2 ， 最多 30s
        backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 30000),
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
        exclude = {JsonProcessingException.class} // 格式錯誤不重試，直接進 DLT
)
public class ConsumerErrorHandler {


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
}
