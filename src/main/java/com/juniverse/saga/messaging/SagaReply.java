package com.juniverse.saga.messaging;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Respuesta de un servicio al orquestador.
 *
 * @param orderId pedido / saga
 * @param step paso al que responde: PAYMENT, INVENTORY o SHIPPING
 * @param action EXECUTE (el paso normal) o COMPENSATE (su compensación)
 * @param success si el servicio pudo hacerlo
 * @param reason motivo del fallo, si lo hubo
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SagaReply(String orderId, String step, String action, boolean success, String reason) {
}
