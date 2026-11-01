package com.juniverse.saga.controller;

import com.juniverse.saga.model.DemoData;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Herramientas de demostración: ver cuentas y stock, restablecer datos y <b>pausar un servicio</b> para simular que está caído. Pausar su
 * consumidor de Kafka es una simulación realista: los mensajes no se pierden, se acumulan en el tema y se procesan al reanudar.
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private static final List<String> SERVICES = List.of("payment", "inventory", "shipping");

    private final DemoData demoData;
    private final JdbcClient jdbc;
    private final KafkaListenerEndpointRegistry registry;

    /**
     * Crea el controlador.
     *
     * @param demoData datos de demostración
     * @param jdbc cliente JDBC
     * @param registry registro de listeners de Kafka
     */
    public AdminController(DemoData demoData, JdbcClient jdbc, KafkaListenerEndpointRegistry registry) {
        this.demoData = demoData;
        this.jdbc = jdbc;
        this.registry = registry;
    }

    /**
     * Restablece cuentas, productos y borra pedidos y sagas.
     *
     * @return confirmación
     */
    @PostMapping("/reset")
    public Map<String, String> reset() {
        demoData.reset();
        return Map.of("status", "reset");
    }

    /**
     * Saldos y existencias actuales.
     *
     * @return cuentas y stock
     */
    @GetMapping("/state")
    public Map<String, Object> state() {
        return Map.of(
                "accounts", jdbc.sql("select customer, balance from accounts order by customer").query((rs, n) -> Map.of("customer", rs.getString(1), "balance", rs.getBigDecimal(2))).list(),
                "stock", jdbc.sql("select sku, stock from products order by sku").query((rs, n) -> Map.of("sku", rs.getString(1), "stock", rs.getInt(2))).list());
    }

    /**
     * Pausa un servicio (simula una caída).
     *
     * @param service payment, inventory o shipping
     * @return estado
     */
    @PostMapping("/services/{service}/pause")
    public Map<String, Object> pause(@PathVariable String service) {
        containersOf(service).forEach(MessageListenerContainer::pause);
        return Map.of("service", service, "paused", true);
    }

    /**
     * Reanuda un servicio.
     *
     * @param service payment, inventory o shipping
     * @return estado
     */
    @PostMapping("/services/{service}/resume")
    public Map<String, Object> resume(@PathVariable String service) {
        containersOf(service).forEach(MessageListenerContainer::resume);
        return Map.of("service", service, "paused", false);
    }

    private List<MessageListenerContainer> containersOf(String service) {
        if (!SERVICES.contains(service)) {
            throw new IllegalArgumentException("Servicio desconocido: " + service + ". Disponibles: " + SERVICES);
        }
        return List.of(registry.getListenerContainer("orch-" + service), registry.getListenerContainer("choreo-" + service));
    }
}
