package com.kiru.microservice.order.repository;

import com.kiru.microservice.order.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, Long> {
    List<Order> findByUserEmail(String userEmail);
}
