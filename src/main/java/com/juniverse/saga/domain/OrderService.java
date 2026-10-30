package com.juniverse.saga.domain;

import com.juniverse.saga.messaging.SagaMessage;
import java.math.BigDecimal;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Servicio de pedidos. Es el dueño del estado del pedido (PENDING, COMPLETED, CANCELLED) y de nadie más: los demás servicios no
 * escriben en sus tablas.
 */
@Service
public class OrderService {

    /**
     * Pedido.
     *
     * @param id identificador
     * @param mode ORCHESTRATION o CHOREOGRAPHY
     * @param customer cliente
     * @param sku producto
     * @param quantity cantidad
     * @param amount importe
     * @param address dirección de envío
     * @param status PENDING, COMPLETED o CANCELLED
     * @param reason motivo de la cancelación, si la hubo
     */
    public record Order(String id, String mode, String customer, String sku, int quantity, BigDecimal amount, String address, String status, String reason) {

        /**
         * Datos del pedido como mensaje de la saga.
         *
         * @param type tipo del mensaje
         * @return mensaje con todos los datos del pedido
         */
        public SagaMessage toMessage(String type) {
            return new SagaMessage(type, id, customer, sku, quantity, amount, address, null);
        }
    }

    private final JdbcClient jdbc;
    private final SagaLog log;

    /**
     * Crea el servicio.
     *
     * @param jdbc cliente JDBC
     * @param log bitácora
     */
    public OrderService(JdbcClient jdbc, SagaLog log) {
        this.jdbc = jdbc;
        this.log = log;
    }

    /**
     * Crea un pedido pendiente calculando su importe con el precio del producto.
     *
     * @param mode ORCHESTRATION o CHOREOGRAPHY
     * @param customer cliente
     * @param sku producto
     * @param quantity cantidad
     * @param address dirección de envío
     * @return el pedido creado
     * @throws IllegalArgumentException si el producto no existe
     */
    @Transactional
    public Order create(String mode, String customer, String sku, int quantity, String address) {
        BigDecimal price = jdbc.sql("select price from products where sku = :sku").param("sku", sku).query(BigDecimal.class).optional()
                .orElseThrow(() -> new IllegalArgumentException("No existe el producto " + sku));
        String id = "ord-" + UUID.randomUUID().toString().substring(0, 8);
        BigDecimal amount = price.multiply(BigDecimal.valueOf(quantity));
        jdbc.sql("insert into orders (id, mode, customer, sku, quantity, amount, address, status) values (:id, :mode, :customer, :sku, :quantity, :amount, :address, 'PENDING')")
                .param("id", id).param("mode", mode).param("customer", customer).param("sku", sku).param("quantity", quantity)
                .param("amount", amount).param("address", address).update();
        log.add(id, "ORDER", "OrderCreated", mode);
        return new Order(id, mode, customer, sku, quantity, amount, address, "PENDING", null);
    }

    /**
     * Busca un pedido.
     *
     * @param id identificador
     * @return el pedido, si existe
     */
    public Optional<Order> find(String id) {
        return jdbc.sql("select * from orders where id = :id").param("id", id).query(OrderService::map).optional();
    }

    /**
     * Busca un pedido que debe existir.
     *
     * @param id identificador
     * @return el pedido
     * @throws NoSuchElementException si no existe
     */
    public Order get(String id) {
        return find(id).orElseThrow(() -> new NoSuchElementException("No existe el pedido " + id));
    }

    /**
     * Pedidos recientes.
     *
     * @param limit cuántos devolver
     * @return pedidos, del más nuevo al más antiguo
     */
    public List<Order> recent(int limit) {
        return jdbc.sql("select * from orders order by created_seq desc limit :limit").param("limit", limit).query(OrderService::map).list();
    }

    private static Order map(java.sql.ResultSet rs, int n) throws java.sql.SQLException {
        return new Order(rs.getString("id"), rs.getString("mode"), rs.getString("customer"), rs.getString("sku"), rs.getInt("quantity"),
                rs.getBigDecimal("amount"), rs.getString("address"), rs.getString("status"), rs.getString("reason"));
    }

    /**
     * Completa el pedido. Es idempotente: solo actúa si estaba pendiente, así que un mensaje repetido no cambia nada.
     *
     * @param orderId pedido
     */
    @Transactional
    public void complete(String orderId) {
        if (jdbc.sql("update orders set status = 'COMPLETED' where id = :id and status = 'PENDING'").param("id", orderId).update() == 1) {
            log.add(orderId, "ORDER", "OrderCompleted", null);
        }
    }

    /**
     * Cancela el pedido. Idempotente: solo actúa si estaba pendiente.
     *
     * @param orderId pedido
     * @param reason motivo
     */
    @Transactional
    public void cancel(String orderId, String reason) {
        if (jdbc.sql("update orders set status = 'CANCELLED', reason = :reason where id = :id and status = 'PENDING'")
                .param("reason", reason).param("id", orderId).update() == 1) {
            log.add(orderId, "ORDER", "OrderCancelled", reason);
        }
    }
}
