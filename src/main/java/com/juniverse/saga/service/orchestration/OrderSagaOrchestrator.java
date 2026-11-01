package com.juniverse.saga.service.orchestration;

import com.juniverse.saga.config.SagaProperties;
import com.juniverse.saga.config.Topics;
import com.juniverse.saga.service.OrderService;
import com.juniverse.saga.model.SagaLog;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaReply;
import com.juniverse.saga.repository.SagaRepository.Saga;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import com.juniverse.saga.repository.SagaRepository;

/**
 * El <b>orquestador</b>: una máquina de estados que conoce el proceso completo y dirige a los servicios con comandos.
 *
 * <pre>
 *   RUNNING ──(todos los pasos bien)──────────────────────────► COMPLETED
 *      │
 *      └─(un paso falla o no responde a tiempo)─► COMPENSATING ─(compensa en orden inverso)─► COMPENSATED
 * </pre>
 *
 * <p>Detalles que importan:</p>
 * <ul>
 *   <li>Cada transición se guarda en la base de datos <b>antes</b> de enviar el siguiente comando (después de confirmar la transacción).
 *       Si el orquestador se cae, al volver retoma las sagas desde su estado guardado.</li>
 *   <li>Una respuesta que no corresponde al paso actual (duplicada o tardía) se <b>ignora</b>.</li>
 *   <li>Cuando un paso falla con una respuesta explícita, ese paso no hizo nada y se compensa desde el anterior. Pero si <b>no responde</b>
 *       (tiempo agotado) no se sabe si llegó a ejecutarse, así que su compensación también se envía: por eso las compensaciones
 *       son idempotentes y funcionan aunque el paso no haya ocurrido.</li>
 *   <li>Las compensaciones no pueden fallar sin consecuencias: si no obtienen respuesta se reintentan; agotados los reintentos la saga
 *       queda <b>STUCK</b> para intervención manual.</li>
 * </ul>
 */
@Component
public class OrderSagaOrchestrator {

    private static final Logger LOG = LoggerFactory.getLogger(OrderSagaOrchestrator.class);

    /** Definición declarativa del proceso: pasos en orden, con su comando y su compensación. */
    public static final List<SagaStep> STEPS = List.of(
            new SagaStep("PAYMENT", Topics.ORCH_PAYMENT_COMMANDS, "ChargePayment", "RefundPayment"),
            new SagaStep("INVENTORY", Topics.ORCH_INVENTORY_COMMANDS, "ReserveStock", "ReleaseStock"),
            new SagaStep("SHIPPING", Topics.ORCH_SHIPPING_COMMANDS, "ScheduleShipment", null));

    private final SagaRepository sagas;
    private final OrderService orders;
    private final SagaLog log;
    private final MessageBus bus;
    private final SagaProperties properties;
    private final Clock clock;

    /**
     * Crea el orquestador.
     *
     * @param sagas estado de las sagas
     * @param orders servicio de pedidos
     * @param log bitácora
     * @param bus bus de mensajes
     * @param properties propiedades
     * @param clock reloj
     */
    public OrderSagaOrchestrator(SagaRepository sagas, OrderService orders, SagaLog log, MessageBus bus, SagaProperties properties, Clock clock) {
        this.sagas = sagas;
        this.orders = orders;
        this.log = log;
        this.bus = bus;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Inicia la saga de un pedido: la guarda y envía el primer comando.
     *
     * @param order pedido recién creado
     */
    @Transactional
    public void start(OrderService.Order order) {
        sagas.insert(order.id(), deadline());
        log.add(order.id(), "ORCHESTRATOR", "SagaStarted", null);
        send(order, STEPS.get(0), STEPS.get(0).command());
    }

    /**
     * Procesa la respuesta de un servicio.
     *
     * @param reply respuesta
     */
    @Transactional
    public void onReply(SagaReply reply) {
        Saga saga = sagas.lock(reply.orderId()).orElse(null);
        int index = indexOf(reply.step());
        if (saga == null || saga.finished() || index != saga.stepIndex() || !expectedAction(saga, reply.action())) {
            LOG.info("Respuesta {}/{} del pedido {} ignorada (duplicada o tardía)", reply.step(), reply.action(), reply.orderId());
            if (saga != null) {
                log.add(reply.orderId(), "ORCHESTRATOR", "ReplyIgnored", reply.step() + "/" + reply.action());
            }
            return;
        }
        OrderService.Order order = orders.get(reply.orderId());
        if (saga.state().equals("RUNNING")) {
            if (reply.success()) {
                advance(saga, order);
            } else {
                log.add(order.id(), "ORCHESTRATOR", "StepFailed", reply.step() + ": " + reply.reason());
                compensateFrom(saga, order, index - 1, reply.reason()); // el paso fallido no hizo nada: se compensa desde el anterior
            }
        } else if (reply.success()) {
            compensateFrom(saga, order, index - 1, null);
        } else {
            retryCompensation(saga, order);
        }
    }

    private static boolean expectedAction(Saga saga, String action) {
        return saga.state().equals("RUNNING") ? "EXECUTE".equals(action) : "COMPENSATE".equals(action);
    }

    private void advance(Saga saga, OrderService.Order order) {
        int next = saga.stepIndex() + 1;
        if (next >= STEPS.size()) {
            sagas.update(order.id(), "COMPLETED", saga.stepIndex(), 0, 0);
            log.add(order.id(), "ORCHESTRATOR", "SagaCompleted", null);
            orders.complete(order.id());
            return;
        }
        sagas.update(order.id(), "RUNNING", next, deadline(), 0);
        send(order, STEPS.get(next), STEPS.get(next).command());
    }

    /** Compensa hacia atrás a partir del paso indicado, saltando los que no tienen compensación. */
    private void compensateFrom(Saga saga, OrderService.Order order, int fromIndex, String reason) {
        int index = fromIndex;
        while (index >= 0 && STEPS.get(index).compensation() == null) {
            index--;
        }
        if (index < 0) {
            sagas.update(order.id(), "COMPENSATED", 0, 0, 0);
            log.add(order.id(), "ORCHESTRATOR", "SagaCompensated", reason);
            orders.cancel(order.id(), reason == null ? readableReason(order.id()) : reason);
            return;
        }
        if (saga.state().equals("RUNNING")) {
            log.add(order.id(), "ORCHESTRATOR", "CompensationStarted", reason);
            saveReason(order.id(), reason);
        }
        sagas.update(order.id(), "COMPENSATING", index, deadline(), 0);
        send(order, STEPS.get(index), STEPS.get(index).compensation());
    }

    private void retryCompensation(Saga saga, OrderService.Order order) {
        if (saga.attempts() + 1 >= properties.maxCompensationAttempts()) {
            sagas.update(order.id(), "STUCK", saga.stepIndex(), 0, saga.attempts() + 1);
            log.add(order.id(), "ORCHESTRATOR", "SagaStuck", "la compensación de " + STEPS.get(saga.stepIndex()).name() + " no se pudo completar");
            LOG.error("Saga del pedido {} ATASCADA: requiere intervención manual", order.id());
            return;
        }
        sagas.update(order.id(), "COMPENSATING", saga.stepIndex(), deadline(), saga.attempts() + 1);
        log.add(order.id(), "ORCHESTRATOR", "CompensationRetried", STEPS.get(saga.stepIndex()).name());
        send(order, STEPS.get(saga.stepIndex()), STEPS.get(saga.stepIndex()).compensation());
    }

    /** Revisa las sagas vencidas (la respuesta esperada no llegó a tiempo). Lo llama el vigilante de tiempos. */
    @Transactional
    public void handleTimeouts() {
        for (String orderId : sagas.expired(clock.millis())) {
            Saga saga = sagas.lock(orderId).orElse(null);
            if (saga == null || saga.finished() || saga.deadline() >= clock.millis()) {
                continue;
            }
            OrderService.Order order = orders.get(orderId);
            if (saga.state().equals("RUNNING")) {
                String step = STEPS.get(saga.stepIndex()).name();
                log.add(orderId, "ORCHESTRATOR", "StepTimedOut", step);
                // Sin respuesta no se sabe si el paso se ejecutó: se compensa INCLUYENDO ese paso
                compensateFrom(saga, order, saga.stepIndex(), "tiempo agotado esperando a " + step);
            } else {
                retryCompensation(saga, order);
            }
        }
    }

    private void send(OrderService.Order order, SagaStep step, String command) {
        boolean compensation = command.equals(step.compensation());
        log.add(order.id(), "ORCHESTRATOR", "CommandSent", command);
        bus.sendAfterCommit(step.commandTopic(), order.id(), order.toMessage(command));
        LOG.debug("{} -> {} ({})", order.id(), command, compensation ? "compensación" : "paso");
    }

    private long deadline() {
        return clock.millis() + properties.stepTimeoutMs();
    }

    private static int indexOf(String stepName) {
        for (int i = 0; i < STEPS.size(); i++) {
            if (STEPS.get(i).name().equals(stepName)) {
                return i;
            }
        }
        return -1;
    }

    private void saveReason(String orderId, String reason) {
        if (reason != null) {
            sagaReasons.put(orderId, reason);
        }
    }

    private String readableReason(String orderId) {
        String reason = sagaReasons.remove(orderId);
        return reason == null ? "saga compensada" : reason;
    }

    /** Motivo que inició la compensación, para usarlo al cancelar el pedido cuando termine (en memoria: solo es informativo). */
    private final java.util.concurrent.ConcurrentHashMap<String, String> sagaReasons = new java.util.concurrent.ConcurrentHashMap<>();
}
