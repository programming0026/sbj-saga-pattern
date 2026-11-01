package com.juniverse.saga.service.orchestration;

import com.juniverse.saga.service.OrderService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Inicia una saga orquestada: crea el pedido y deja que el orquestador tome el control. */
@Service
public class OrchestrationStarter {

    private final OrderService orders;
    private final OrderSagaOrchestrator orchestrator;

    /**
     * Crea el iniciador.
     *
     * @param orders servicio de pedidos
     * @param orchestrator orquestador
     */
    public OrchestrationStarter(OrderService orders, OrderSagaOrchestrator orchestrator) {
        this.orders = orders;
        this.orchestrator = orchestrator;
    }

    /**
     * Crea el pedido (y su saga) en una sola transacción.
     *
     * @param customer cliente
     * @param sku producto
     * @param quantity cantidad
     * @param address dirección de envío
     * @return el pedido
     */
    @Transactional
    public OrderService.Order start(String customer, String sku, int quantity, String address) {
        OrderService.Order order = orders.create("ORCHESTRATION", customer, sku, quantity, address);
        orchestrator.start(order);
        return order;
    }
}
