package com.juniverse.saga.orchestration;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.domain.InventoryService;
import com.juniverse.saga.domain.Outcome;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.messaging.SagaMessage;
import com.juniverse.saga.messaging.SagaReply;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Participante INVENTARIO de la orquestación. */
@Component
public class InventoryParticipant {

    private final InventoryService inventory;
    private final MessageBus bus;

    /**
     * Crea el participante.
     *
     * @param inventory servicio de inventario
     * @param bus bus de mensajes
     */
    public InventoryParticipant(InventoryService inventory, MessageBus bus) {
        this.inventory = inventory;
        this.bus = bus;
    }

    /**
     * Atiende un comando.
     *
     * @param payload JSON del comando
     */
    @KafkaListener(id = "orch-inventory", groupId = "orch-inventory", topics = Topics.ORCH_INVENTORY_COMMANDS)
    public void on(String payload) {
        SagaMessage command = bus.readMessage(payload);
        switch (command.type()) {
            case "ReserveStock" -> reply(command, "EXECUTE", inventory.reserve(command.orderId(), command.sku(), command.quantity()));
            case "ReleaseStock" -> {
                inventory.release(command.orderId(), command.sku(), command.quantity());
                reply(command, "COMPENSATE", Outcome.OK);
            }
            default -> { }
        }
    }

    private void reply(SagaMessage command, String action, Outcome outcome) {
        bus.send(Topics.ORCH_REPLIES, command.orderId(), new SagaReply(command.orderId(), "INVENTORY", action, outcome.success(), outcome.reason()));
    }
}
