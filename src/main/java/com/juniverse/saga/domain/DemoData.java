package com.juniverse.saga.domain;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Datos de demostración: cuentas y productos con los que provocar cada escenario. */
@Component
public class DemoData {

    private final JdbcClient jdbc;

    /**
     * Crea el componente.
     *
     * @param jdbc cliente JDBC
     */
    public DemoData(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Borra pedidos, sagas y movimientos, y restablece cuentas y productos:
     * <ul>
     *   <li>Clientes: {@code ana} (1000), {@code eva} (5000), {@code luis} (50: no le alcanza para casi nada) y {@code rich} (1.000.000).</li>
     *   <li>Productos: {@code LAPTOP} (800, stock 10), {@code MOUSE} (20, stock 100) y {@code SCARCE} (10, stock 5: se agota rápido).</li>
     * </ul>
     */
    @Transactional
    public void reset() {
        for (String table : new String[] {"saga_log", "sagas", "shipments", "reservations", "payments", "orders", "accounts", "products"}) {
            jdbc.sql("delete from " + table).update();
        }
        account("ana", 1000);
        account("eva", 5000);
        account("luis", 50);
        account("rich", 1_000_000);
        product("LAPTOP", 800, 10);
        product("MOUSE", 20, 100);
        product("SCARCE", 10, 5);
    }

    private void account(String customer, long balance) {
        jdbc.sql("insert into accounts (customer, balance) values (:customer, :balance)").param("customer", customer).param("balance", balance).update();
    }

    private void product(String sku, long price, int stock) {
        jdbc.sql("insert into products (sku, price, stock) values (:sku, :price, :stock)").param("sku", sku).param("price", price).param("stock", stock).update();
    }
}
