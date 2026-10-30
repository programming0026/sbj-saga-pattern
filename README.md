# Patrón Saga con Spring Boot 4 y Kafka: orquestación y coreografía

Ejemplo con **Spring Boot 4**, **Java 21** y **Apache Kafka** (con Docker) del patrón **Saga**, implementado de las **dos formas clásicas** sobre el mismo proceso de negocio para que puedas compararlas lado a lado: **orquestación** (un coordinador dirige) y **coreografía** (cada servicio reacciona a los demás).

## El problema

Un pedido necesita **cobrar** (servicio de pagos), **reservar stock** (inventario) y **programar el envío** (envíos). Cada servicio tiene su propia base de datos, así que **no existe una transacción que abarque los tres**. ¿Qué pasa si el pago y la reserva salen bien y el envío falla? Hay dinero cobrado y stock apartado de un pedido que no se va a entregar.

## La solución: una saga

Una saga es una **secuencia de transacciones locales**; si una falla, no se hace *rollback* (ya están confirmadas) sino que se ejecutan **transacciones compensatorias** que deshacen las anteriores **en orden inverso**.

| Paso | Acción | Compensación |
|------|--------|--------------|
| 1. Pagos | Cobrar | Reembolsar |
| 2. Inventario | Reservar stock | Liberar stock |
| 3. Envíos | Programar envío | *(ninguna: es el último paso)* |

```text
Camino feliz:   cobrar ──► reservar ──► enviar ──► PEDIDO COMPLETADO
Falla el envío: cobrar ──► reservar ──✗ enviar
                              liberar stock ◄──┘
                reembolsar ◄──┘                 ──► PEDIDO CANCELADO
```

## Orquestación frente a coreografía

```text
ORQUESTACIÓN                                    COREOGRAFÍA

              ┌────────────────┐                OrderCreated ──► [Pagos] ── PaymentCompleted ──► [Inventario]
   comandos   │  ORQUESTADOR   │  respuestas                                                          │
  ┌──────────►│ (máquina de    │◄──────────┐    [Pedidos] ◄── ShipmentScheduled ◄── [Envíos] ◄── StockReserved
  │           │  estados)      │           │         ▲
  ▼           └────────────────┘           │         └── PaymentFailed / PaymentRefunded (cancelar)
[Pagos]  [Inventario]  [Envíos] ───────────┘
```

| | Orquestación | Coreografía |
|---|---|---|
| ¿Quién conoce el proceso completo? | El orquestador (una sola clase con los pasos declarados) | Nadie: está repartido en las reacciones de cada servicio |
| Mensajes | **Comandos** ("cobra esto") y **respuestas** | **Eventos** ("se cobró") |
| Acoplamiento | Los servicios no se conocen entre sí, solo al orquestador | Cada servicio conoce los eventos de los demás |
| Añadir/cambiar un paso | Se toca el orquestador | Se tocan varios servicios |
| Ver el estado de un pedido | Una tabla (`sagas`) | Hay que reconstruirlo uniendo eventos (aquí, con una bitácora) |
| **Tiempos de espera** | El orquestador los vigila y compensa | **Nadie vigila**: si un servicio no responde, la saga se queda esperando |
| Riesgo | El orquestador puede volverse un "dios" con demasiada lógica y es un punto delicado | Dependencias cíclicas entre servicios; difícil de entender con muchos pasos |
| Ideal para | Procesos largos, con muchos pasos, ramas y reglas | Procesos cortos con pocos participantes |

## Lo que hay que hacer bien en cualquier saga (y el ejemplo hace)

| Problema | Solución en el código |
|----------|-----------------------|
| Kafka entrega **"al menos una vez"**: los mensajes pueden llegar repetidos | Todas las operaciones son **idempotentes**: la clave primaria es el id del pedido (`payments`, `reservations`, `shipments`). Se inserta la fila **primero**; si ya existe, se devuelve el resultado anterior. Dos mensajes simultáneos del mismo pedido no pueden cobrar dos veces |
| Una compensación llega **antes** que su acción (mensajes rezagados) | La compensación deja una **"lápida"** (`REFUNDED`/`RELEASED`) aunque no hubiera nada que deshacer. Si el cobro o la reserva llegan tarde, se **rechazan** (`LateChargeRejected`) en lugar de quedar huérfanos |
| Un paso **no responde**: ¿se ejecutó o no? | Tras el tiempo de espera el orquestador compensa **incluyendo ese paso**. Como las compensaciones son idempotentes y tienen lápida, es seguro aunque nunca se hubiera ejecutado |
| Dos pedidos compiten por el último producto | La resta de stock es **una sola sentencia con su condición** (`stock >= cantidad`); nunca queda negativo |
| Se reinicia el orquestador | Su estado vive en la tabla `sagas`; cada transición se guarda antes de enviar el siguiente comando |
| Respuesta duplicada o tardía al orquestador | Se compara con el paso y la acción esperados; si no coinciden, se ignora y se anota `ReplyIgnored` |
| Una compensación no obtiene respuesta | Se reintenta; agotados `max-compensation-attempts` la saga queda `STUCK` para intervención manual (una compensación no puede "darse por perdida") |
| Orden de los mensajes de un pedido | La clave del mensaje es el id del pedido: van a la misma partición y se procesan en orden |
| **Mensaje veneno** (JSON corrupto) | Se reintenta si procede y, si no, va al tema `<tema>.DLT` (cartas muertas) sin bloquear los siguientes |
| No anunciar algo que se deshizo | Los mensajes se publican **después** de confirmar la transacción de base de datos |
| Estados finales | Una orden solo cambia de `PENDING` a `COMPLETED` o `CANCELLED` una vez (actualización condicional) |

> **Limitación consciente:** entre confirmar la base de datos y publicar en Kafka puede caerse el proceso. Aquí lo absorbe la idempotencia al reintentar, pero el primer mensaje de una saga (al crear el pedido) podría perderse. La solución completa es el patrón **Transactional Outbox** (mira el ejemplo `sbj-outbox-pattern`).

## Estructura

```text
src/main/java/com/juniverse/saga/
├── domain/                     # la lógica de negocio, igual para ambos enfoques
│   ├── PaymentService          # cobrar / reembolsar (idempotentes, con lápida)
│   ├── InventoryService        # reservar / liberar
│   ├── ShippingService         # programar envío
│   ├── OrderService            # estado del pedido
│   └── SagaLog                 # bitácora de todo lo que ocurre
├── choreography/               # 4 participantes que reaccionan a eventos
│   ├── ChoreographyStarter, PaymentChoreography, InventoryChoreography,
│   └── ShippingChoreography, OrderChoreography
├── orchestration/
│   ├── OrderSagaOrchestrator   # la máquina de estados (RUNNING → COMPLETED | COMPENSATING → COMPENSATED)
│   ├── SagaStep                # definición declarativa de cada paso y su compensación
│   ├── SagaRepository          # estado persistido (con bloqueo de fila)
│   ├── OrchestratorListeners   # respuestas de Kafka y vigilante de tiempos
│   └── PaymentParticipant, InventoryParticipant, ShippingParticipant   # ejecutan comandos y responden
├── messaging/                  # MessageBus, SagaMessage (evento/comando), SagaReply
├── config/                     # temas de Kafka, manejo de errores y cartas muertas
└── web/                        # API de pedidos y herramientas de demostración
```

Los participantes de ambos enfoques usan **los mismos servicios de dominio**: lo único que cambia es quién decide qué viene después.

## Requisitos

- JDK 21, Maven 3.9 o superior y Docker.

## Ejecutar

```bash
docker compose up -d          # Kafka 4 (KRaft, un solo nodo)
mvn spring-boot:run
```

Datos de demostración (se restablecen con `POST /api/admin/reset`):

| Clientes | Saldo | | Productos | Precio | Stock |
|---|---|---|---|---|---|
| `ana` | 1.000 | | `LAPTOP` | 800 | 10 |
| `eva` | 5.000 | | `MOUSE` | 20 | 100 |
| `luis` | 50 | | `SCARCE` | 10 | 5 |
| `rich` | 1.000.000 | | | | |

Cada escenario se provoca con los datos del pedido (cambia `orchestration` por `choreography` para usar el otro enfoque):

```bash
# Camino feliz
curl -X POST http://localhost:8080/api/orchestration/orders -H "Content-Type: application/json" \
     -d '{"customer":"ana","sku":"MOUSE","quantity":2,"address":"Calle 1"}'

# Falla el PAGO (luis no tiene saldo para un LAPTOP)
curl -X POST http://localhost:8080/api/orchestration/orders -H "Content-Type: application/json" \
     -d '{"customer":"luis","sku":"LAPTOP","quantity":1,"address":"Calle 1"}'

# Falla el INVENTARIO (solo hay 5 SCARCE): se reembolsa el pago
curl -X POST http://localhost:8080/api/orchestration/orders -H "Content-Type: application/json" \
     -d '{"customer":"ana","sku":"SCARCE","quantity":6,"address":"Calle 1"}'

# Falla el ENVÍO (dirección que empieza por INVALID): se libera el stock y se reembolsa
curl -X POST http://localhost:8080/api/orchestration/orders -H "Content-Type: application/json" \
     -d '{"customer":"ana","sku":"LAPTOP","quantity":1,"address":"INVALID address"}'

# Ver el resultado y la bitácora de cada paso
curl http://localhost:8080/api/orders/<orderId>
curl http://localhost:8080/api/admin/state        # saldos y stock: ¿volvieron a su sitio?
```

Salida real de la bitácora cuando falla el envío (`> `separa los pasos):

```text
ORDER:OrderCreated > PAYMENT:PaymentCompleted > INVENTORY:StockReserved > SHIPPING:ShipmentFailed
  > INVENTORY:StockReleased > PAYMENT:PaymentRefunded > ORDER:OrderCancelled
```

### El experimento más interesante: un servicio caído

```bash
curl -X POST http://localhost:8080/api/admin/services/inventory/pause      # inventario "se cae"
curl -X POST http://localhost:8080/api/orchestration/orders -H "Content-Type: application/json" \
     -d '{"customer":"eva","sku":"MOUSE","quantity":1,"address":"Calle 1"}'
# espera unos segundos y consulta el pedido...
curl http://localhost:8080/api/admin/services/inventory/resume
```

- **Orquestación:** tras `saga.step-timeout-ms` el orquestador se impacienta, reembolsa el pago y pide liberar el stock. Al volver inventario procesa primero el `ReserveStock` rezagado y **enseguida** el `ReleaseStock` de la compensación (mismo tema, misma clave, mismo orden), la respuesta tardía se ignora y la saga termina `COMPENSATED` con todo en su sitio.
- **Coreografía:** nadie vigila el tiempo, así que el pedido queda `PENDING` mientras inventario esté caído; como Kafka conserva los mensajes, al reanudar la saga continúa sola. Es la diferencia práctica más importante entre ambos enfoques.

(Pausar el consumidor de Kafka es una simulación realista: los mensajes no se pierden, se acumulan en el tema.)

### Configuración

| Propiedad | Descripción |
|-----------|-------------|
| `saga.step-timeout-ms` | Cuánto espera el orquestador la respuesta de un paso (15 s por defecto) |
| `saga.watch-interval-ms` | Cada cuánto revisa las sagas vencidas |
| `saga.max-compensation-attempts` | Reintentos de una compensación antes de marcar la saga `STUCK` |

## Pruebas

```bash
mvn verify
```

16 pruebas de integración contra un **Kafka real** (Testcontainers; requieren Docker). Cada una comprueba la **secuencia exacta** de pasos y compensaciones y que el dinero y el stock terminen en su sitio:

- **Coreografía (5):** camino feliz, falla el pago, falla el inventario, falla el envío (compensación en cadena) y un servicio caído.
- **Orquestación (5):** los cuatro escenarios anteriores y el servicio caído con la compensación en orden correcto.
- **Transversales (6):** **12 pedidos simultáneos** de un producto con 5 en stock mezclando ambos enfoques (se completan exactamente 5, el dinero cuadra y el stock nunca baja de 0); **idempotencia** ante mensajes repetidos; **lápida** (compensación antes que la acción); **respuestas tardías** al orquestador ignoradas; **mensaje veneno** en la cola de cartas muertas sin bloquear el flujo; y la API.

## Para producción

- Cada servicio como **aplicación y base de datos independientes** (aquí comparten proceso y una H2 en memoria para poder ejecutarlo con un comando).
- Añade el **Transactional Outbox** para no perder el primer mensaje, y esquemas versionados (Avro/Protobuf con Schema Registry) en lugar de un mensaje JSON único.
- Monitorea las sagas `STUCK`, las que llevan mucho tiempo `COMPENSATING` y los mensajes en las colas `.DLT`.
- Define compensaciones **semánticas**: algunas acciones no se pueden deshacer (un correo enviado); se compensa con otra acción (un correo de disculpa).
- Considera motores de flujo de trabajo (Temporal, Camunda, Axon) cuando las sagas se vuelvan largas y numerosas.

Fat jar ejecutable: `mvn clean package` y `java -jar target/sbj-saga-pattern-0.0.1-SNAPSHOT.jar`.

## Licencia

Código de ejemplo de uso libre con fines educativos.
