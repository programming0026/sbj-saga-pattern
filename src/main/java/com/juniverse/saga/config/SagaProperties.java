package com.juniverse.saga.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Propiedades {@code saga.*}.
 *
 * @param stepTimeoutMs tiempo máximo que el orquestador espera la respuesta de un paso antes de compensar
 * @param watchIntervalMs cada cuántos milisegundos el orquestador revisa las sagas vencidas
 * @param maxCompensationAttempts reintentos de una compensación sin respuesta antes de marcar la saga como atascada (requiere intervención manual)
 */
@ConfigurationProperties(prefix = "saga")
public record SagaProperties(long stepTimeoutMs, long watchIntervalMs, int maxCompensationAttempts) {
}
