package com.juniverse.saga.messaging;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;
import com.juniverse.saga.dto.SagaMessage;
import com.juniverse.saga.dto.SagaReply;

/** Envío y lectura de mensajes JSON. Centraliza la serialización para que ningún servicio toque Kafka directamente. */
@Component
public class MessageBus {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;

    /**
     * Crea el bus.
     *
     * @param kafka plantilla de Kafka
     * @param json serializador JSON
     */
    public MessageBus(KafkaTemplate<String, String> kafka, JsonMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    /**
     * Publica un mensaje y espera la confirmación del broker. La clave (id del pedido) garantiza el orden de los mensajes de un mismo pedido.
     *
     * @param topic tema
     * @param key clave
     * @param message mensaje (se serializa a JSON)
     */
    public void send(String topic, String key, Object message) {
        try {
            kafka.send(new ProducerRecord<>(topic, key, json.writeValueAsString(message))).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Envío interrumpido", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("No se pudo publicar en " + topic + ": " + e.getMessage(), e);
        }
    }

    /**
     * Publica un mensaje DESPUÉS de que la transacción de base de datos en curso se confirme (o de inmediato si no hay ninguna). Así nunca
     * se anuncia un cambio que luego se deshizo.
     *
     * @param topic tema
     * @param key clave
     * @param message mensaje
     */
    public void sendAfterCommit(String topic, String key, Object message) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send(topic, key, message);
                }
            });
        } else {
            send(topic, key, message);
        }
    }

    /**
     * Lee un mensaje de la saga.
     *
     * @param payload JSON recibido
     * @return el mensaje
     * @throws tools.jackson.core.JacksonException si el JSON es ilegible (el manejador de errores lo envía a la cola de cartas muertas)
     */
    public SagaMessage readMessage(String payload) {
        return json.readValue(payload, SagaMessage.class);
    }

    /**
     * Lee una respuesta al orquestador.
     *
     * @param payload JSON recibido
     * @return la respuesta
     */
    public SagaReply readReply(String payload) {
        return json.readValue(payload, SagaReply.class);
    }
}
