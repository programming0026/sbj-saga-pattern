package com.juniverse.saga.web;

import com.juniverse.saga.choreography.ChoreographyStarter;
import com.juniverse.saga.domain.OrderService;
import com.juniverse.saga.domain.SagaLog;
import com.juniverse.saga.orchestration.OrchestrationStarter;
import com.juniverse.saga.orchestration.SagaRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
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

    /**
     * Datos de un pedido.
     *
     * @param customer cliente (ana, eva, luis, rich)
     * @param sku producto (LAPTOP, MOUSE, SCARCE)
     * @param quantity cantidad
     * @param address dirección de envío; si empieza por INVALID, el envío falla
     */
    public record OrderRequest(@NotBlank(message = "El cliente es obligatorio") String customer, @NotBlank(message = "El producto es obligatorio") String sku,
            @Min(value = 1, message = "La cantidad mínima es 1") int quantity, String address) {
    }

    /**
     * Pedido con su bitácora.
     *
     * @param order datos del pedido
     * @param sagaState estado del orquestador (solo en orquestación)
     * @param log bitácora de la saga
     */
    public record OrderView(OrderService.Order order, String sagaState, List<SagaLog.Entry> log) {
    }

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
