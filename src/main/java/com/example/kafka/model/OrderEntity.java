package com.example.kafka.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "kafka_orders")
public class OrderEntity {

    @Id
    @Column(name = "order_id")
    private String orderId;
    private String product;
    private double amount;
}
