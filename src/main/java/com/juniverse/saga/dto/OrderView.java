package com.juniverse.saga.dto;

import com.juniverse.saga.model.SagaLog;
import com.juniverse.saga.service.OrderService;
import java.util.List;

/**
 * Pedido con su bitácora.
 *
 * @param order datos del pedido
 * @param sagaState estado del orquestador (solo en orquestación)
 * @param log bitácora de la saga
 */
public record OrderView(OrderService.Order order, String sagaState, List<SagaLog.Entry> log) {
}
