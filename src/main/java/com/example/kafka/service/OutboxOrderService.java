package com.example.kafka.service;

import com.example.kafka.model.Order;
import com.example.kafka.model.OrderEntity;
import com.example.kafka.model.OutboxEntity;
import com.example.kafka.repository.OrderRepository;
import com.example.kafka.repository.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxOrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;


    public void createOrder(Order order) throws JsonProcessingException {
        OrderEntity orderEntity = OrderEntity.builder()
                .orderId(order.getOrderId())
                .amount(order.getAmount())
                .product(order.getProduct())
                .build();

        orderRepository.save(orderEntity);

        OutboxEntity outbox = OutboxEntity.builder()
                .aggregateType("Order")
                .aggregateId(order.getOrderId())
                .type("OrderCreated")
                .payload(objectMapper.writeValueAsString(order))
                .build();

        outboxRepository.save(outbox);

    }


}
