package com.juniverse.saga.controller;

import com.juniverse.saga.dto.OrderRequest;
import com.juniverse.saga.dto.OrderView;
import com.juniverse.saga.service.choreography.ChoreographyStarter;
import com.juniverse.saga.service.OrderService;
import com.juniverse.saga.model.SagaLog;
import com.juniverse.saga.service.orchestration.OrchestrationStarter;
import com.juniverse.saga.repository.SagaRepository;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** API de pedidos: se crean con cualquiera de los dos enfoques y se consultan igual. Responden 202 porque el resultado llega después. */
@RestController
@RequestMapping("/api")
public class OrderController {

    private final OrchestrationStarter orchestration;
    private final ChoreographyStarter choreography;
    private final OrderService orders;
    private final SagaRepository sagas;
    private final SagaLog log;

    /**
     * Crea el controlador.
     *
     * @param orchestration iniciador de sagas orquestadas
     * @param choreography iniciador de sagas coreografiadas
     * @param orders servicio de pedidos
     * @param sagas estado de las sagas
     * @param log bitácora
     */
    public OrderController(OrchestrationStarter orchestration, ChoreographyStarter choreography, OrderService orders, SagaRepository sagas, SagaLog log) {
        this.orchestration = orchestration;
        this.choreography = choreography;
        this.orders = orders;
        this.sagas = sagas;
        this.log = log;
    }

    /**
     * Crea un pedido con ORQUESTACIÓN.
     *
     * @param request datos validados
     * @return 202 con el id del pedido
     */
    @PostMapping("/orchestration/orders")
    public ResponseEntity<Map<String, String>> orchestrated(@Valid @RequestBody OrderRequest request) {
        OrderService.Order order = orchestration.start(request.customer(), request.sku(), request.quantity(), request.address());
        return accepted(order);
    }

    /**
     * Crea un pedido con COREOGRAFÍA.
     *
     * @param request datos validados
     * @return 202 con el id del pedido
     */
    @PostMapping("/choreography/orders")
    public ResponseEntity<Map<String, String>> choreographed(@Valid @RequestBody OrderRequest request) {
        OrderService.Order order = choreography.start(request.customer(), request.sku(), request.quantity(), request.address());
        return accepted(order);
    }

    private static ResponseEntity<Map<String, String>> accepted(OrderService.Order order) {
        return ResponseEntity.accepted().location(java.net.URI.create("/api/orders/" + order.id()))
                .body(Map.of("orderId", order.id(), "mode", order.mode(), "status", order.status()));
    }

    /**
     * Consulta un pedido con su bitácora.
     *
     * @param id pedido
     * @return pedido, estado de la saga y bitácora
     */
    @GetMapping("/orders/{id}")
    public OrderView get(@PathVariable String id) {
        OrderService.Order order = orders.get(id);
        return new OrderView(order, sagas.find(id).map(SagaRepository.Saga::state).orElse(null), log.of(id));
    }

    /**
     * Pedidos recientes.
     *
     * @param limit cuántos (1 a 100)
     * @return pedidos
     */
    @GetMapping("/orders")
    public List<OrderService.Order> recent(@RequestParam(defaultValue = "20") int limit) {
        return orders.recent(Math.min(Math.max(limit, 1), 100));
    }
}
