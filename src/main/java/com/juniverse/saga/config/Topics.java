package com.juniverse.saga.config;

/** Nombres de los temas de Kafka. Cada enfoque tiene los suyos para que convivan sin mezclarse. */
public final class Topics {

    /** Coreografía: eventos del servicio de pedidos. */
    public static final String CHOREO_ORDER = "choreo.order-events";
    /** Coreografía: eventos del servicio de pagos. */
    public static final String CHOREO_PAYMENT = "choreo.payment-events";
    /** Coreografía: eventos del servicio de inventario. */
    public static final String CHOREO_INVENTORY = "choreo.inventory-events";
    /** Coreografía: eventos del servicio de envíos. */
    public static final String CHOREO_SHIPPING = "choreo.shipping-events";

    /** Orquestación: comandos al servicio de pagos. */
    public static final String ORCH_PAYMENT_COMMANDS = "orch.payment-commands";
    /** Orquestación: comandos al servicio de inventario. */
    public static final String ORCH_INVENTORY_COMMANDS = "orch.inventory-commands";
    /** Orquestación: comandos al servicio de envíos. */
    public static final String ORCH_SHIPPING_COMMANDS = "orch.shipping-commands";
    /** Orquestación: respuestas de los servicios al orquestador. */
    public static final String ORCH_REPLIES = "orch.replies";

    private Topics() {
    }
}
