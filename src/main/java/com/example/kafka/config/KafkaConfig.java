package com.example.kafka.config;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import javax.sql.DataSource;

@Configuration
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(0L, 0));
    }

    // 1. 定義 Kafka 事務管理器
    // 預設的自動配置只能讓 Kafka 自己管自己。但在你的案例中，
    // 你需要 MySQL 跟 Kafka 同步，這時候你就必須手動介入，把這兩個經理「綁在一起」
    // 負責把 Kafka 變成「可事務化的工具」
    @Bean
    public KafkaTransactionManager<String, String> kafkaTransactionManager(ProducerFactory<String, String> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    // 這是告訴 資料庫經理 (JPA)：「從現在起，你就是老大 (Primary)。不論你做什麼（提交或回滾），都要同步發簡訊通知其他小弟（例如 Kafka 經理）。」
    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        JpaTransactionManager manager = new JpaTransactionManager(entityManagerFactory);
        // 這行是核心：允許將其他事務（如 Kafka）同步到這個資料庫事務中
        manager.setTransactionSynchronization(JpaTransactionManager.SYNCHRONIZATION_ALWAYS);
        return manager;
    }

//    @Bean
//    @Primary
//    public DataSourceTransactionManager transactionManager(DataSource dataSource) {
//        DataSourceTransactionManager manager = new DataSourceTransactionManager(dataSource);
//
//        manager.setTransactionSynchronization(DataSourceTransactionManager.SYNCHRONIZATION_ALWAYS);
//
//        return manager;
//    }
}


