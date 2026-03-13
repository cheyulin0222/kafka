package com.example.kafka.controller;

//import com.example.kafka.service.OrderService;
import com.example.kafka.service.OrderService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class Test {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OrderService orderService;

    @GetMapping("/test1")
    public void test1() {
        log.info("發送一筆");
        String topic = "orders";

        kafkaTemplate.send(topic, 0, "test-key", "test");

    }

//    @GetMapping("/createOrder")
//    public void createOrder(@RequestParam String orderId, @RequestParam String product, @RequestParam double amount) throws JsonProcessingException, InterruptedException {
//        orderService.createOrderWithSync(orderId, product, amount);
//    }


}
