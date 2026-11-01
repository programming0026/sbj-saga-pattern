package com.juniverse.saga.service.choreography;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.service.OrderService;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Participante PEDIDOS de la coreografía: cierra la saga. Reacciona a:
 * <ul>
 *   <li>{@code ShipmentScheduled} → el pedido queda COMPLETED;</li>
 *   <li>{@code PaymentFailed} y {@code PaymentRefunded} → el pedido queda CANCELLED (ya no queda nada por deshacer).</li>
 * </ul>
 */
@Component
public class OrderChoreography {

    private final OrderService orders;
    private final MessageBus bus;

    /**
     * Crea el participante.
     *
     * @param orders servicio de pedidos
     * @param bus bus de mensajes
     */
    public OrderChoreography(OrderService orders, MessageBus bus) {
        this.orders = orders;
        this.bus = bus;
    }

    /**
     * Atiende los eventos que le interesan.
     *
     * @param payload mensaje JSON
     */
    @KafkaListener(id = "choreo-order", groupId = "choreo-order", topics = {Topics.CHOREO_PAYMENT, Topics.CHOREO_SHIPPING})
    public void on(String payload) {
        SagaMessage event = bus.readMessage(payload);
        switch (event.type()) {
            case "ShipmentScheduled" -> orders.complete(event.orderId());
            case "PaymentFailed", "PaymentRefunded" -> orders.cancel(event.orderId(), event.reason());
            default -> { }
        }
    }
}
