package com.juniverse.saga.service.orchestration;

import com.juniverse.saga.config.Topics;
import com.juniverse.saga.messaging.MessageBus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Entradas del orquestador: las respuestas de los servicios (Kafka) y el vigilante de tiempos (planificador). */
@Component
public class OrchestratorListeners {

    private final OrderSagaOrchestrator orchestrator;
    private final MessageBus bus;

    /**
     * Crea los listeners.
     *
     * @param orchestrator orquestador
     * @param bus bus de mensajes
     */
    public OrchestratorListeners(OrderSagaOrchestrator orchestrator, MessageBus bus) {
        this.orchestrator = orchestrator;
        this.bus = bus;
    }

    /**
     * Recibe una respuesta de un servicio.
     *
     * @param payload JSON de la respuesta
     */
    @KafkaListener(id = "orch-replies", groupId = "orch-orchestrator", topics = Topics.ORCH_REPLIES)
    public void onReply(String payload) {
        orchestrator.onReply(bus.readReply(payload));
    }

    /** Vigila los tiempos de espera de las sagas en curso. */
    @Scheduled(fixedDelayString = "${saga.watch-interval-ms}")
    public void watch() {
        orchestrator.handleTimeouts();
    }
}
