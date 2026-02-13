package com.example.kafka.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name = "process_message")
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ProcessMessage {

    @Id
    @Column(name = "order_id")
    private String orderId;
}
