package com.juniverse.saga.domain;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Servicio de inventario, dueño del stock. Reservar y liberar son idempotentes y la liberación deja "lápida" (ver {@link PaymentService}). */
@Service
public class InventoryService {

    private final JdbcClient jdbc;
    private final SagaLog log;

    /**
     * Crea el servicio.
     *
     * @param jdbc cliente JDBC
     * @param log bitácora
     */
    public InventoryService(JdbcClient jdbc, SagaLog log) {
        this.jdbc = jdbc;
        this.log = log;
    }

    /**
     * Reserva stock para un pedido. La resta es una sola sentencia con su condición ({@code stock >= cantidad}): dos pedidos simultáneos
     * nunca dejan el stock en negativo.
     *
     * @param orderId pedido (clave de idempotencia)
     * @param sku producto
     * @param quantity cantidad
     * @return éxito, o el motivo del rechazo
     */
    @Transactional
    public Outcome reserve(String orderId, String sku, int quantity) {
        try {
            jdbc.sql("insert into reservations (order_id, sku, quantity, status) values (:order, :sku, :quantity, 'PROCESSING')")
                    .param("order", orderId).param("sku", sku).param("quantity", quantity).update();
        } catch (DuplicateKeyException e) {
            return previousOutcome(orderId);
        }
        int reserved = jdbc.sql("update products set stock = stock - :quantity where sku = :sku and stock >= :quantity")
                .param("quantity", quantity).param("sku", sku).update();
        if (reserved == 1) {
            jdbc.sql("update reservations set status = 'RESERVED' where order_id = :order").param("order", orderId).update();
            log.add(orderId, "INVENTORY", "StockReserved", sku + " x" + quantity);
            return Outcome.OK;
        }
        jdbc.sql("update reservations set status = 'REJECTED' where order_id = :order").param("order", orderId).update();
        log.add(orderId, "INVENTORY", "StockRejected", "stock insuficiente de " + sku);
        return Outcome.failed("stock insuficiente de " + sku);
    }

    private Outcome previousOutcome(String orderId) {
        String status = jdbc.sql("select status from reservations where order_id = :order").param("order", orderId).query(String.class).single();
        return switch (status) {
            case "RESERVED" -> Outcome.OK;
            case "RELEASED" -> {
                log.add(orderId, "INVENTORY", "LateReservationRejected", "la reserva llegó después de su compensación");
                yield Outcome.failed("la reserva ya fue cancelada");
            }
            default -> Outcome.failed("stock insuficiente");
        };
    }

    /**
     * Compensación: devuelve el stock reservado. Idempotente, y segura aunque la reserva aún no haya llegado.
     *
     * @param orderId pedido
     * @param sku producto
     * @param quantity cantidad
     */
    @Transactional
    public void release(String orderId, String sku, int quantity) {
        try {
            jdbc.sql("insert into reservations (order_id, sku, quantity, status) values (:order, :sku, :quantity, 'RELEASED')")
                    .param("order", orderId).param("sku", sku).param("quantity", quantity).update();
            log.add(orderId, "INVENTORY", "StockReleased", "sin reserva previa");
            return;
        } catch (DuplicateKeyException ignored) {
            // ya existía la fila: se sigue abajo
        }
        if (jdbc.sql("update reservations set status = 'RELEASED' where order_id = :order and status = 'RESERVED'").param("order", orderId).update() == 1) {
            jdbc.sql("update products set stock = stock + :quantity where sku = :sku").param("quantity", quantity).param("sku", sku).update();
            log.add(orderId, "INVENTORY", "StockReleased", sku + " x" + quantity);
        }
    }
}
