package com.example.kafka.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@Table(name = "outbox")
@AllArgsConstructor
@NoArgsConstructor
public class OutboxEntity {

    @Id
    private Integer id;

    private String topic;

    private String payload;

    private String status;

}
