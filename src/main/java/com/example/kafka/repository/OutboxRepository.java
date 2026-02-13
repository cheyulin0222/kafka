package com.example.kafka.repository;

import com.example.kafka.model.OutboxEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;


public interface OutboxRepository extends JpaRepository<OutboxEntity, Integer> {

    List<OutboxEntity> findByStatus(String status);
    void updateStatusById(Integer id, String status);
}
