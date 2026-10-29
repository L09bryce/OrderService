package com.kiru.microservice.order.controller;

import com.kiru.microservice.order.dto.OrderRequest;
import com.kiru.microservice.order.model.Order;
import com.kiru.microservice.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderController {
    private final OrderService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public String placeOrder(@Valid @RequestBody OrderRequest orderRequest) {
        orderService.placeOrder(orderRequest);
        return "Order Placed Successfully";
    }

    @GetMapping
    public List<Order> findOrdersByUserEmail(@RequestParam("userEmail") String userEmail) {
        return orderService.findOrdersByUserEmail(userEmail);
    }

    @ExceptionHandler
    public ResponseEntity<String> handleValidationException(Exception ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }
}