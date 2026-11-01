package com.juniverse.saga.service.orchestration;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.model.Outcome;
import com.juniverse.saga.service.ShippingService;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaMessage;
import com.juniverse.saga.dto.SagaReply;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Participante ENVÍOS de la orquestación (último paso, sin compensación). */
@Component
public class ShippingParticipant {

    private final ShippingService shipping;
    private final MessageBus bus;

    /**
     * Crea el participante.
     *
     * @param shipping servicio de envíos
     * @param bus bus de mensajes
     */
    public ShippingParticipant(ShippingService shipping, MessageBus bus) {
        this.shipping = shipping;
        this.bus = bus;
    }

    /**
     * Atiende un comando.
     *
     * @param payload JSON del comando
     */
    @KafkaListener(id = "orch-shipping", groupId = "orch-shipping", topics = Topics.ORCH_SHIPPING_COMMANDS)
    public void on(String payload) {
        SagaMessage command = bus.readMessage(payload);
        if ("ScheduleShipment".equals(command.type())) {
            Outcome outcome = shipping.schedule(command.orderId(), command.address());
            bus.send(Topics.ORCH_REPLIES, command.orderId(), new SagaReply(command.orderId(), "SHIPPING", "EXECUTE", outcome.success(), outcome.reason()));
        }
    }
}
