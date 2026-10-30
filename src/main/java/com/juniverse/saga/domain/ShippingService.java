package com.juniverse.saga.domain;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Servicio de envíos. Es el último paso de la saga: si falla se compensan los anteriores, y por eso él no necesita compensación. */
@Service
public class ShippingService {

    private final JdbcClient jdbc;
    private final SagaLog log;

    /**
     * Crea el servicio.
     *
     * @param jdbc cliente JDBC
     * @param log bitácora
     */
    public ShippingService(JdbcClient jdbc, SagaLog log) {
        this.jdbc = jdbc;
        this.log = log;
    }

    /**
     * Programa el envío. Regla de negocio de ejemplo: no se puede enviar a una dirección vacía o que empiece por {@code INVALID}.
     *
     * @param orderId pedido (clave de idempotencia)
     * @param address dirección
     * @return éxito, o el motivo del rechazo
     */
    @Transactional
    public Outcome schedule(String orderId, String address) {
        boolean valid = address != null && !address.isBlank() && !address.toUpperCase().startsWith("INVALID");
        try {
            jdbc.sql("insert into shipments (order_id, address, status) values (:order, :address, :status)")
                    .param("order", orderId).param("address", address == null ? "" : address).param("status", valid ? "SCHEDULED" : "FAILED").update();
        } catch (DuplicateKeyException e) {
            String status = jdbc.sql("select status from shipments where order_id = :order").param("order", orderId).query(String.class).single();
            return "SCHEDULED".equals(status) ? Outcome.OK : Outcome.failed("dirección no válida");
        }
        if (valid) {
            log.add(orderId, "SHIPPING", "ShipmentScheduled", address);
            return Outcome.OK;
        }
        log.add(orderId, "SHIPPING", "ShipmentFailed", "dirección no válida");
        return Outcome.failed("dirección no válida");
    }
}
