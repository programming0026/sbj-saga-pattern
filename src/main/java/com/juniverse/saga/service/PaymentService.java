package com.juniverse.saga.service;

import java.math.BigDecimal;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.juniverse.saga.model.Outcome;
import com.juniverse.saga.model.SagaLog;

/**
 * Servicio de pagos, dueño de las cuentas y de los cobros. Sus dos operaciones son la acción y su compensación, y las dos son
 * <b>idempotentes</b>: Kafka puede entregar un mensaje más de una vez y repetir un cobro no debe cobrar dos veces.
 *
 * <p>Truco importante: el REEMBOLSO deja una "lápida" ({@code REFUNDED}) incluso si todavía no hubo cobro. Si el comando de reembolso
 * llegara antes que un cobro rezagado, ese cobro posterior se rechazaría en lugar de quedar huérfano.</p>
 */
@Service
public class PaymentService {

    private final JdbcClient jdbc;
    private final SagaLog log;

    /**
     * Crea el servicio.
     *
     * @param jdbc cliente JDBC
     * @param log bitácora
     */
    public PaymentService(JdbcClient jdbc, SagaLog log) {
        this.jdbc = jdbc;
        this.log = log;
    }

    /**
     * Cobra un pedido.
     *
     * @param orderId pedido (clave de idempotencia)
     * @param customer cliente
     * @param amount importe
     * @return éxito, o el motivo del rechazo
     */
    @Transactional
    public Outcome charge(String orderId, String customer, BigDecimal amount) {
        try {
            // La fila se inserta PRIMERO: la clave primaria es la barrera contra dos cobros simultáneos del mismo pedido
            jdbc.sql("insert into payments (order_id, customer, amount, status) values (:order, :customer, :amount, 'PROCESSING')")
                    .param("order", orderId).param("customer", customer).param("amount", amount).update();
        } catch (DuplicateKeyException e) {
            return previousOutcome(orderId);
        }
        int debited = jdbc.sql("update accounts set balance = balance - :amount where customer = :customer and balance >= :amount")
                .param("amount", amount).param("customer", customer).update();
        if (debited == 1) {
            jdbc.sql("update payments set status = 'CHARGED' where order_id = :order").param("order", orderId).update();
            log.add(orderId, "PAYMENT", "PaymentCompleted", amount.toPlainString());
            return Outcome.OK;
        }
        String reason = jdbc.sql("select count(*) from accounts where customer = :customer").param("customer", customer).query(Long.class).single() == 0
                ? "cuenta inexistente" : "saldo insuficiente";
        jdbc.sql("update payments set status = 'FAILED', reason = :reason where order_id = :order").param("reason", reason).param("order", orderId).update();
        log.add(orderId, "PAYMENT", "PaymentFailed", reason);
        return Outcome.failed(reason);
    }

    private Outcome previousOutcome(String orderId) {
        var row = jdbc.sql("select status, reason from payments where order_id = :order").param("order", orderId)
                .query((rs, n) -> new String[] {rs.getString("status"), rs.getString("reason")}).single();
        return switch (row[0]) {
            case "CHARGED" -> Outcome.OK;
            case "REFUNDED" -> {
                log.add(orderId, "PAYMENT", "LateChargeRejected", "el cobro llegó después de su compensación");
                yield Outcome.failed("el cobro ya fue cancelado");
            }
            default -> Outcome.failed(row[1] == null ? "cobro en proceso" : row[1]);
        };
    }

    /**
     * Compensación: devuelve el dinero si se había cobrado. Es seguro llamarla varias veces y también ANTES de que llegue el cobro.
     *
     * @param orderId pedido
     * @param customer cliente
     * @param amount importe
     */
    @Transactional
    public void refund(String orderId, String customer, BigDecimal amount) {
        try {
            // Si no había cobro, se deja la lápida para que un cobro tardío sea rechazado
            jdbc.sql("insert into payments (order_id, customer, amount, status) values (:order, :customer, :amount, 'REFUNDED')")
                    .param("order", orderId).param("customer", customer).param("amount", amount).update();
            log.add(orderId, "PAYMENT", "PaymentRefunded", "sin cobro previo");
            return;
        } catch (DuplicateKeyException ignored) {
            // ya existía la fila: se sigue abajo
        }
        if (jdbc.sql("update payments set status = 'REFUNDED' where order_id = :order and status = 'CHARGED'").param("order", orderId).update() == 1) {
            jdbc.sql("update accounts set balance = balance + :amount where customer = :customer").param("amount", amount).param("customer", customer).update();
            log.add(orderId, "PAYMENT", "PaymentRefunded", amount.toPlainString());
        }
    }
}
