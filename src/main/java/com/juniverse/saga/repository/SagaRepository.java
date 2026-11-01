package com.juniverse.saga.repository;

import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Estado persistido de las sagas. Que esté en base de datos permite que el orquestador se reinicie sin perder ninguna saga en curso. */
@Repository
public class SagaRepository {

    /**
     * Estado de una saga.
     *
     * @param orderId pedido
     * @param state RUNNING, COMPENSATING, COMPLETED, COMPENSATED o STUCK
     * @param stepIndex paso que se está ejecutando o compensando
     * @param deadline límite (milisegundos desde 1970) para recibir la respuesta esperada
     * @param attempts reintentos de la compensación en curso
     */
    public record Saga(String orderId, String state, int stepIndex, long deadline, int attempts) {

        public boolean finished() {
            return state.equals("COMPLETED") || state.equals("COMPENSATED") || state.equals("STUCK");
        }
    }

    private final JdbcClient jdbc;

    /**
     * Crea el repositorio.
     *
     * @param jdbc cliente JDBC
     */
    public SagaRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    private static Saga map(java.sql.ResultSet rs, int n) throws java.sql.SQLException {
        return new Saga(rs.getString("order_id"), rs.getString("state"), rs.getInt("step_index"), rs.getLong("deadline"), rs.getInt("attempts"));
    }

    /**
     * Crea una saga.
     *
     * @param orderId pedido
     * @param deadline límite de la primera respuesta
     */
    public void insert(String orderId, long deadline) {
        jdbc.sql("insert into sagas (order_id, state, step_index, deadline, attempts) values (:order, 'RUNNING', 0, :deadline, 0)")
                .param("order", orderId).param("deadline", deadline).update();
    }

    /**
     * Lee una saga BLOQUEANDO su fila hasta el fin de la transacción: así una respuesta y el vigilante de tiempos nunca la modifican a la vez.
     *
     * @param orderId pedido
     * @return la saga, si existe
     */
    public Optional<Saga> lock(String orderId) {
        return jdbc.sql("select * from sagas where order_id = :order for update").param("order", orderId).query(SagaRepository::map).optional();
    }

    /**
     * Lee una saga sin bloquearla.
     *
     * @param orderId pedido
     * @return la saga, si existe
     */
    public Optional<Saga> find(String orderId) {
        return jdbc.sql("select * from sagas where order_id = :order").param("order", orderId).query(SagaRepository::map).optional();
    }

    /**
     * Actualiza el estado de una saga.
     *
     * @param orderId pedido
     * @param state nuevo estado
     * @param stepIndex nuevo paso
     * @param deadline nuevo límite
     * @param attempts reintentos
     */
    public void update(String orderId, String state, int stepIndex, long deadline, int attempts) {
        jdbc.sql("update sagas set state = :state, step_index = :step, deadline = :deadline, attempts = :attempts where order_id = :order")
                .param("state", state).param("step", stepIndex).param("deadline", deadline).param("attempts", attempts).param("order", orderId).update();
    }

    /**
     * Sagas en curso cuyo límite ya pasó.
     *
     * @param now hora actual en milisegundos
     * @return identificadores de pedido
     */
    public List<String> expired(long now) {
        return jdbc.sql("select order_id from sagas where state in ('RUNNING', 'COMPENSATING') and deadline < :now").param("now", now).query(String.class).list();
    }
}
