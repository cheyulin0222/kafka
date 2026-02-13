package com.example.kafka.repository;

import com.example.kafka.model.ProcessMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessMessage, String> {
}
