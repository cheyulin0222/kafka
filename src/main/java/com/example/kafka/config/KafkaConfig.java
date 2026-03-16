package com.example.kafka.config;

import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        // 設定重試間隔 1 秒，最多重試 3 次
        DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        // 哪些 Exception 不需要重試
        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }

    // 也可以透過 application.properties 配置
    // 1. 外部包裝：使用 ErrorHandlingDeserializer 確保不崩潰
    // spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
    // 2. 內部核心：告訴保鏢，真正解析 JSON 的人是誰
    // spring.kafka.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer
    // 3. 信任 Package (JSON 必備)
    // spring.kafka.properties.spring.json.trusted.packages=*
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        // 要去連哪一台 Kafka server
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 屬於哪一個團隊
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");

        JsonDeserializer<Object> payloadDeserializer = new JsonDeserializer<>();
        payloadDeserializer.addTrustedPackages("*");

        ErrorHandlingDeserializer<Object> errorHandlingDeserializer =
                new ErrorHandlingDeserializer<>(payloadDeserializer);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer
        );
    }

    // 1. 定義 Kafka 事務管理器
    // 預設的自動配置只能讓 Kafka 自己管自己。但在你的案例中，
    // 你需要 MySQL 跟 Kafka 同步，這時候你就必須手動介入，把這兩個經理「綁在一起」
    // 負責把 Kafka 變成「可事務化的工具」
    @Bean
    public PlatformTransactionManager kafkaTransactionManager(ProducerFactory<String, String> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    // 這是告訴 資料庫經理 (JPA)：
    // 「從現在起，你就是老大 (Primary)。不論你做什麼（提交或回滾）
    // 都要同步發簡訊通知其他小弟（例如 Kafka 經理）。」
    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        JpaTransactionManager manager = new JpaTransactionManager(entityManagerFactory);
        // 核心：這是對講機開關，讓 JPA 在 Commit 後會去通知 Kafka
        // 強迫 JPA 在完成自己的工作後，必須去檢查並完成其他掛載在他身上的事務
        manager.setTransactionSynchronization(JpaTransactionManager.SYNCHRONIZATION_ALWAYS);
        return manager;
    }

    // 假設沒有使用 JPA ，就使用這個做為 TransactionManager
    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        DataSourceTransactionManager manager = new DataSourceTransactionManager(dataSource);

        manager.setTransactionSynchronization(DataSourceTransactionManager.SYNCHRONIZATION_ALWAYS);

        return manager;
    }
}


