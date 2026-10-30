package com.juniverse.saga.domain;

import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/** Bitácora de la saga: cada paso relevante deja una línea. Permite ver cómo avanzó (o cómo se compensó) un pedido. */
@Component
public class SagaLog {

    /**
     * Línea de la bitácora.
     *
     * @param source quién lo registró (ORDER, PAYMENT, INVENTORY, SHIPPING u ORCHESTRATOR)
     * @param event qué ocurrió
     * @param detail detalle opcional
     */
    public record Entry(String source, String event, String detail) {
    }

    private final JdbcClient jdbc;

    /**
     * Crea la bitácora.
     *
     * @param jdbc cliente JDBC
     */
    public SagaLog(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Anota un evento.
     *
     * @param orderId pedido
     * @param source quién lo registra
     * @param event qué ocurrió
     * @param detail detalle opcional
     */
    public void add(String orderId, String source, String event, String detail) {
        jdbc.sql("insert into saga_log (order_id, source, event, detail) values (:order, :source, :event, :detail)")
                .param("order", orderId).param("source", source).param("event", event).param("detail", detail).update();
    }

    /**
     * Bitácora de un pedido en orden cronológico.
     *
     * @param orderId pedido
     * @return líneas
     */
    public List<Entry> of(String orderId) {
        return jdbc.sql("select source, event, detail from saga_log where order_id = :order order by id").param("order", orderId)
                .query((rs, n) -> new Entry(rs.getString("source"), rs.getString("event"), rs.getString("detail"))).list();
    }
}
