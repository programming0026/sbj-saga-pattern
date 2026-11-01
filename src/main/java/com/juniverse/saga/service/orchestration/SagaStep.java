package com.juniverse.saga.service.orchestration;

/**
 * Un paso de la saga: a qué servicio se le pide, con qué comando y cuál es el comando que lo compensa.
 *
 * @param name nombre del paso (PAYMENT, INVENTORY, SHIPPING)
 * @param commandTopic tema donde el servicio escucha sus comandos
 * @param command comando que ejecuta el paso
 * @param compensation comando que lo deshace, o {@code null} si no necesita compensación
 */
public record SagaStep(String name, String commandTopic, String command, String compensation) {
}
