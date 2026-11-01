package com.juniverse.saga.service.orchestration;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.model.Outcome;
import com.juniverse.saga.service.PaymentService;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaMessage;
import com.juniverse.saga.dto.SagaReply;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Participante PAGOS de la orquestación: no decide nada, ejecuta los comandos que recibe y responde al orquestador. */
@Component
public class PaymentParticipant {

    private final PaymentService payments;
    private final MessageBus bus;

    /**
     * Crea el participante.
     *
     * @param payments servicio de pagos
     * @param bus bus de mensajes
     */
    public PaymentParticipant(PaymentService payments, MessageBus bus) {
        this.payments = payments;
        this.bus = bus;
    }

    /**
     * Atiende un comando.
     *
     * @param payload JSON del comando
     */
    @KafkaListener(id = "orch-payment", groupId = "orch-payment", topics = Topics.ORCH_PAYMENT_COMMANDS)
    public void on(String payload) {
        SagaMessage command = bus.readMessage(payload);
        switch (command.type()) {
            case "ChargePayment" -> reply(command, "EXECUTE", payments.charge(command.orderId(), command.customer(), command.amount()));
            case "RefundPayment" -> {
                payments.refund(command.orderId(), command.customer(), command.amount());
                reply(command, "COMPENSATE", Outcome.OK);
            }
            default -> { }
        }
    }

    private void reply(SagaMessage command, String action, Outcome outcome) {
        bus.send(Topics.ORCH_REPLIES, command.orderId(), new SagaReply(command.orderId(), "PAYMENT", action, outcome.success(), outcome.reason()));
    }
}
