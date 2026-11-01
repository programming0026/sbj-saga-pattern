package com.juniverse.saga.service.choreography;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.service.OrderService;
import com.juniverse.saga.messaging.MessageBus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Inicia una saga coreografiada: crea el pedido y publica {@code OrderCreated}. A partir de ahí nadie dirige nada: cada servicio
 * reacciona a los eventos que le interesan.
 */
@Service
public class ChoreographyStarter {

    private final OrderService orders;
    private final MessageBus bus;

    /**
     * Crea el iniciador.
     *
     * @param orders servicio de pedidos
     * @param bus bus de mensajes
     */
    public ChoreographyStarter(OrderService orders, MessageBus bus) {
        this.orders = orders;
        this.bus = bus;
    }

    /**
     * Crea el pedido y anuncia su creación.
     *
     * @param customer cliente
     * @param sku producto
     * @param quantity cantidad
     * @param address dirección de envío
     * @return el pedido
     */
    @Transactional
    public OrderService.Order start(String customer, String sku, int quantity, String address) {
        OrderService.Order order = orders.create("CHOREOGRAPHY", customer, sku, quantity, address);
        bus.sendAfterCommit(Topics.CHOREO_ORDER, order.id(), order.toMessage("OrderCreated"));
        return order;
    }
}
