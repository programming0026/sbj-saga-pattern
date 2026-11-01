package com.juniverse.saga.service.choreography;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.model.Outcome;
import com.juniverse.saga.service.ShippingService;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Participante ENVÍOS de la coreografía. Reacciona a {@code StockReserved} → programa el envío y publica {@code ShipmentScheduled} o {@code ShipmentFailed}. */
@Component
public class ShippingChoreography {

    private final ShippingService shipping;
    private final MessageBus bus;

    /**
     * Crea el participante.
     *
     * @param shipping servicio de envíos
     * @param bus bus de mensajes
     */
    public ShippingChoreography(ShippingService shipping, MessageBus bus) {
        this.shipping = shipping;
        this.bus = bus;
    }

    /**
     * Atiende los eventos que le interesan.
     *
     * @param payload mensaje JSON
     */
    @KafkaListener(id = "choreo-shipping", groupId = "choreo-shipping", topics = Topics.CHOREO_INVENTORY)
    public void on(String payload) {
        SagaMessage event = bus.readMessage(payload);
        if ("StockReserved".equals(event.type())) {
            Outcome outcome = shipping.schedule(event.orderId(), event.address());
            bus.send(Topics.CHOREO_SHIPPING, event.orderId(), outcome.success() ? event.as("ShipmentScheduled") : event.as("ShipmentFailed", outcome.reason()));
        }
    }
}
