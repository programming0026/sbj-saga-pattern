package com.juniverse.saga.service.choreography;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.model.Outcome;
import com.juniverse.saga.service.PaymentService;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Participante PAGOS de la coreografía. Reacciona a:
 * <ul>
 *   <li>{@code OrderCreated} → cobra y publica {@code PaymentCompleted} o {@code PaymentFailed};</li>
 *   <li>{@code StockRejected} / {@code StockReleased} → compensa: reembolsa y publica {@code PaymentRefunded}.</li>
 * </ul>
 */
@Component
public class PaymentChoreography {

    private final PaymentService payments;
    private final MessageBus bus;

    /**
     * Crea el participante.
     *
     * @param payments servicio de pagos
     * @param bus bus de mensajes
     */
    public PaymentChoreography(PaymentService payments, MessageBus bus) {
        this.payments = payments;
        this.bus = bus;
    }

    /**
     * Atiende los eventos que le interesan.
     *
     * @param payload mensaje JSON
     */
    @KafkaListener(id = "choreo-payment", groupId = "choreo-payment", topics = {Topics.CHOREO_ORDER, Topics.CHOREO_INVENTORY})
    public void on(String payload) {
        SagaMessage event = bus.readMessage(payload);
        switch (event.type()) {
            case "OrderCreated" -> {
                Outcome outcome = payments.charge(event.orderId(), event.customer(), event.amount());
                bus.send(Topics.CHOREO_PAYMENT, event.orderId(), outcome.success() ? event.as("PaymentCompleted") : event.as("PaymentFailed", outcome.reason()));
            }
            case "StockRejected", "StockReleased" -> {
                payments.refund(event.orderId(), event.customer(), event.amount());
                bus.send(Topics.CHOREO_PAYMENT, event.orderId(), event.as("PaymentRefunded", event.reason()));
            }
            default -> { } // otros eventos no le interesan
        }
    }
}
