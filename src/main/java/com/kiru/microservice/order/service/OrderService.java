package com.kiru.microservice.order.service;

import com.kiru.microservice.order.client.InventoryClient;
import com.kiru.microservice.order.dto.OrderRequest;
//import com.kiru.microservice.order.event.OrderPlacedEvent;
import com.kiru.microservice.order.event.OrderPlacedEvent;
import com.kiru.microservice.order.model.Order;
import com.kiru.microservice.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

import static com.kiru.microservice.order.client.InventoryClient.log;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;

    public void placeOrder(OrderRequest orderRequest) {
        // Validate OrderRequest
        if (orderRequest == null || orderRequest.skuCode() == null || orderRequest.quantity() == null
                || orderRequest.userDetails() == null
                || orderRequest.userDetails().email() == null
                || orderRequest.userDetails().email().isBlank()) {
            throw new IllegalArgumentException("Invalid OrderRequest: SKU Code, Quantity, and User Email must not be null or blank.");
        }

        // Check product availability
        var isProductInStock = inventoryClient.isInStock(orderRequest.skuCode(), orderRequest.quantity());


        if (isProductInStock) {
            // Create and save the order
            Order order = new Order();
            order.setOrderNumber(UUID.randomUUID().toString());
            order.setUserEmail(orderRequest.userDetails().email());
            order.setPrice(orderRequest.price());
            order.setSkuCode(orderRequest.skuCode());
            order.setQuantity(orderRequest.quantity());
            orderRepository.save(order);

            // Send event to Kafka
            OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent();
            orderPlacedEvent.setOrderNumber(order.getOrderNumber());
            orderPlacedEvent.setEmail(orderRequest.userDetails().email());
            orderPlacedEvent.setFirstName(orderRequest.userDetails().firstName());
            orderPlacedEvent.setLastName(orderRequest.userDetails().lastName());
            log.info("Start - Sending OrderPlacedEvent {} to Kafka topic order-placed", orderPlacedEvent);
            try {
                kafkaTemplate.send("order-placed-topic", orderPlacedEvent)
                        .whenComplete((result, exception) -> {
                            if (exception != null) {
                                log.error("Failed to send OrderPlacedEvent {} to Kafka topic order-placed",
                                        orderPlacedEvent, exception);
                                return;
                            }

                            log.info("OrderPlacedEvent {} sent to Kafka topic order-placed, partition {}, offset {}",
                                    orderPlacedEvent,
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        });
                log.info("Kafka send initiated for OrderPlacedEvent {}", orderPlacedEvent);
            } catch (Exception exception) {
                log.error("Kafka send failed synchronously for OrderPlacedEvent {}",
                        orderPlacedEvent, exception);
                throw exception;
            }
        } else {
            throw new RuntimeException("Product with SKU Code " + orderRequest.skuCode() + " is not in stock.");
        }
    }

    public List<Order> findOrdersByUserEmail(String userEmail) {
        if (userEmail == null || userEmail.isBlank()) {
            throw new IllegalArgumentException("User email must not be null or blank.");
        }
        return orderRepository.findByUserEmail(userEmail);
    }
}