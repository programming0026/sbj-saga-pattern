package com.juniverse.saga.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

/** Temas de Kafka y manejo de mensajes que no se pueden procesar. */
@Configuration
public class KafkaConfig {

    private static NewTopic topic(String name) {
        // Tres particiones: los mensajes de un mismo pedido llevan su id como clave, así van siempre a la misma partición y se
        // procesan EN ORDEN, mientras que pedidos distintos se reparten entre particiones y se procesan en paralelo.
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic choreoOrderTopic() {
        return topic(Topics.CHOREO_ORDER);
    }

    @Bean
    NewTopic choreoPaymentTopic() {
        return topic(Topics.CHOREO_PAYMENT);
    }

    @Bean
    NewTopic choreoInventoryTopic() {
        return topic(Topics.CHOREO_INVENTORY);
    }

    @Bean
    NewTopic choreoShippingTopic() {
        return topic(Topics.CHOREO_SHIPPING);
    }

    @Bean
    NewTopic orchPaymentCommandsTopic() {
        return topic(Topics.ORCH_PAYMENT_COMMANDS);
    }

    @Bean
    NewTopic orchInventoryCommandsTopic() {
        return topic(Topics.ORCH_INVENTORY_COMMANDS);
    }

    @Bean
    NewTopic orchShippingCommandsTopic() {
        return topic(Topics.ORCH_SHIPPING_COMMANDS);
    }

    @Bean
    NewTopic orchRepliesTopic() {
        return topic(Topics.ORCH_REPLIES);
    }

    /**
     * Qué hacer cuando un mensaje falla al procesarse: se reintenta dos veces y, si sigue fallando, se envía a un tema "de cartas
     * muertas" ({@code <tema>.DLT}) para revisarlo después. Sin esto, un mensaje corrupto ("mensaje veneno") se reintentaría para siempre
     * y bloquearía todos los que vienen detrás en esa partición. Un JSON ilegible no se reintenta: reintentarlo no lo arreglaría.
     *
     * @param template plantilla de Kafka
     * @return manejador de errores que Spring Boot aplica a todos los listeners
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer((KafkaOperations<?, ?>) template,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", -1)); // -1: que Kafka elija la partición
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 2));
        handler.addNotRetryableExceptions(JacksonException.class);
        return handler;
    }
}
