package com.juniverse.saga.service.choreography;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.service.InventoryService;
import com.juniverse.saga.model.Outcome;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Participante INVENTARIO de la coreografía. Reacciona a:
 * <ul>
 *   <li>{@code PaymentCompleted} → reserva y publica {@code StockReserved} o {@code StockRejected};</li>
 *   <li>{@code ShipmentFailed} → compensa: libera el stock y publica {@code StockReleased}.</li>
 * </ul>
 */
@Component
public class InventoryChoreography {

    private final InventoryService inventory;
    private final MessageBus bus;

    /**
     * Crea el participante.
     *
     * @param inventory servicio de inventario
     * @param bus bus de mensajes
     */
    public InventoryChoreography(InventoryService inventory, MessageBus bus) {
        this.inventory = inventory;
        this.bus = bus;
    }

    /**
     * Atiende los eventos que le interesan.
     *
     * @param payload mensaje JSON
     */
    @KafkaListener(id = "choreo-inventory", groupId = "choreo-inventory", topics = {Topics.CHOREO_PAYMENT, Topics.CHOREO_SHIPPING})
    public void on(String payload) {
        SagaMessage event = bus.readMessage(payload);
        switch (event.type()) {
            case "PaymentCompleted" -> {
                Outcome outcome = inventory.reserve(event.orderId(), event.sku(), event.quantity());
                bus.send(Topics.CHOREO_INVENTORY, event.orderId(), outcome.success() ? event.as("StockReserved") : event.as("StockRejected", outcome.reason()));
            }
            case "ShipmentFailed" -> {
                inventory.release(event.orderId(), event.sku(), event.quantity());
                bus.send(Topics.CHOREO_INVENTORY, event.orderId(), event.as("StockReleased", event.reason()));
            }
            default -> { }
        }
    }
}
