package com.juniverse.saga.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.math.BigDecimal;

/**
 * Mensaje de la saga: sirve tanto de EVENTO (coreografía: "algo ocurrió") como de COMANDO (orquestación: "haz esto"). Lleva los datos
 * del pedido completos para que cada servicio pueda actuar sin consultar a los demás (<i>event-carried state transfer</i>).
 *
 * @param type tipo de evento o comando, por ejemplo {@code PaymentCompleted} o {@code ChargePayment}
 * @param orderId pedido (también es la clave del mensaje en Kafka)
 * @param customer cliente
 * @param sku producto
 * @param quantity cantidad
 * @param amount importe total
 * @param address dirección de envío
 * @param reason motivo, cuando el mensaje comunica un fallo
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SagaMessage(String type, String orderId, String customer, String sku, int quantity, BigDecimal amount, String address, String reason) {

    /**
     * Copia con otro tipo, para reenviar los mismos datos como el siguiente evento de la cadena.
     *
     * @param newType nuevo tipo
     * @return mensaje con el tipo cambiado y sin motivo
     */
    public SagaMessage as(String newType) {
        return new SagaMessage(newType, orderId, customer, sku, quantity, amount, address, null);
    }

    /**
     * Copia con otro tipo y un motivo.
     *
     * @param newType nuevo tipo
     * @param why motivo del fallo
     * @return mensaje con el tipo y motivo indicados
     */
    public SagaMessage as(String newType, String why) {
        return new SagaMessage(newType, orderId, customer, sku, quantity, amount, address, why);
    }
}
