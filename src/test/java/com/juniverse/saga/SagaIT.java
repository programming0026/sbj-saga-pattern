package com.juniverse.saga;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.juniverse.saga.config.Topics;
import com.juniverse.saga.model.DemoData;
import com.juniverse.saga.model.SagaLog;
import com.juniverse.saga.messaging.MessageBus;
import com.juniverse.saga.dto.SagaMessage;
import com.juniverse.saga.dto.SagaReply;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Las dos sagas (orquestación y coreografía) contra un Kafka real de Testcontainers (requiere Docker). Cada escenario comprueba la
 * SECUENCIA exacta de pasos y compensaciones y que el dinero y el stock terminen donde deben.
 */
@SpringBootTest(properties = {"saga.step-timeout-ms=2500", "saga.watch-interval-ms=200", "saga.max-compensation-attempts=3"})
@Testcontainers(disabledWithoutDocker = true)
@DisplayName("Patrón Saga: orquestación y coreografía")
class SagaIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.0.0");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired private WebApplicationContext context;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private DemoData demoData;
    @Autowired private SagaLog sagaLog;
    @Autowired private MessageBus bus;
    @Autowired private KafkaListenerEndpointRegistry registry;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
        resumeAll();
        awaitQuiet();
        demoData.reset();
    }

    @AfterEach
    void tearDown() {
        resumeAll();
    }

    // ------------------------------------------------------------------ utilidades
    private void resumeAll() {
        for (String id : List.of("orch-payment", "orch-inventory", "orch-shipping", "choreo-payment", "choreo-inventory", "choreo-shipping")) {
            MessageListenerContainer container = registry.getListenerContainer(id);
            if (container != null && container.isPauseRequested()) {
                container.resume();
            }
        }
    }

    /** Espera a que no queden pedidos ni sagas en curso de la prueba anterior. */
    private void awaitQuiet() {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() ->
                jdbc.queryForObject("select count(*) from orders where status = 'PENDING'", Integer.class) == 0
                        && jdbc.queryForObject("select count(*) from sagas where state in ('RUNNING','COMPENSATING')", Integer.class) == 0);
    }

    private void pause(String service) {
        for (String id : List.of("orch-" + service, "choreo-" + service)) {
            MessageListenerContainer container = registry.getListenerContainer(id);
            container.pause();
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(container::isContainerPaused);
        }
    }

    private String create(String mode, String customer, String sku, int quantity, String address) throws Exception {
        String body = mvc.perform(post("/api/" + mode + "/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"customer\":\"" + customer + "\",\"sku\":\"" + sku + "\",\"quantity\":" + quantity + ",\"address\":\"" + address + "\"}"))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        return JsonPath.read(body, "$.orderId");
    }

    private String statusOf(String orderId) {
        return jdbc.queryForObject("select status from orders where id = ?", String.class, orderId);
    }

    private String awaitFinished(String orderId) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> !statusOf(orderId).equals("PENDING"));
        return statusOf(orderId);
    }

    /** Eventos de los servicios de negocio, sin los del orquestador, en orden. */
    private List<String> businessEvents(String orderId) {
        return sagaLog.of(orderId).stream().filter(e -> !e.source().equals("ORCHESTRATOR")).map(e -> e.source() + ":" + e.event()).toList();
    }

    private List<String> orchestratorEvents(String orderId) {
        return sagaLog.of(orderId).stream().filter(e -> e.source().equals("ORCHESTRATOR")).map(SagaLog.Entry::event).toList();
    }

    private BigDecimal balance(String customer) {
        return jdbc.queryForObject("select balance from accounts where customer = ?", BigDecimal.class, customer);
    }

    private int stock(String sku) {
        return jdbc.queryForObject("select stock from products where sku = ?", Integer.class, sku);
    }

    private void assertMoney(String customer, long expected) {
        assertEquals(0, BigDecimal.valueOf(expected).compareTo(balance(customer)), "saldo de " + customer + " = " + balance(customer));
    }

    // ================================================================== COREOGRAFÍA
    @Test
    @DisplayName("COREOGRAFÍA, camino feliz: pedido → pago → inventario → envío → completado")
    void choreographyHappyPath() throws Exception {
        String id = create("choreography", "ana", "MOUSE", 2, "Calle 1");
        assertEquals("COMPLETED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentCompleted", "INVENTORY:StockReserved", "SHIPPING:ShipmentScheduled", "ORDER:OrderCompleted"), businessEvents(id));
        assertMoney("ana", 960);
        assertEquals(98, stock("MOUSE"));
    }

    @Test
    @DisplayName("COREOGRAFÍA, falla el pago: no hay nada que compensar")
    void choreographyPaymentFails() throws Exception {
        String id = create("choreography", "luis", "LAPTOP", 1, "Calle 1");
        assertEquals("CANCELLED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentFailed", "ORDER:OrderCancelled"), businessEvents(id));
        assertMoney("luis", 50);
        assertEquals(10, stock("LAPTOP"));
    }

    @Test
    @DisplayName("COREOGRAFÍA, falla el inventario: se reembolsa el pago")
    void choreographyStockFails() throws Exception {
        String id = create("choreography", "ana", "SCARCE", 6, "Calle 1");
        assertEquals("CANCELLED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentCompleted", "INVENTORY:StockRejected", "PAYMENT:PaymentRefunded", "ORDER:OrderCancelled"), businessEvents(id));
        assertMoney("ana", 1000);
        assertEquals(5, stock("SCARCE"));
    }

    @Test
    @DisplayName("COREOGRAFÍA, falla el envío: se libera el stock y DESPUÉS se reembolsa (compensación en cadena)")
    void choreographyShippingFails() throws Exception {
        String id = create("choreography", "ana", "LAPTOP", 1, "INVALID address");
        assertEquals("CANCELLED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentCompleted", "INVENTORY:StockReserved", "SHIPPING:ShipmentFailed", "INVENTORY:StockReleased",
                "PAYMENT:PaymentRefunded", "ORDER:OrderCancelled"), businessEvents(id));
        assertMoney("ana", 1000);
        assertEquals(10, stock("LAPTOP"));
    }

    @Test
    @DisplayName("COREOGRAFÍA con un servicio caído: los mensajes no se pierden y la saga continúa al volver (pero nadie vigila el tiempo)")
    void choreographyWaitsForADownService() throws Exception {
        pause("inventory");
        String id = create("choreography", "ana", "MOUSE", 1, "Calle 1");

        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> sagaLog.of(id).stream().anyMatch(e -> e.event().equals("PaymentCompleted")));
        Thread.sleep(3_000); // más que el tiempo de espera del orquestador: aquí no hay ninguno que se impaciente
        assertEquals("PENDING", statusOf(id), "sin coordinador, el pedido queda pendiente indefinidamente mientras el servicio esté caído");
        assertMoney("ana", 980);

        registry.getListenerContainer("choreo-inventory").resume();
        assertEquals("COMPLETED", awaitFinished(id));
    }

    // ================================================================== ORQUESTACIÓN
    @Test
    @DisplayName("ORQUESTACIÓN, camino feliz: el orquestador envía un comando tras otro")
    void orchestrationHappyPath() throws Exception {
        String id = create("orchestration", "ana", "MOUSE", 2, "Calle 1");
        assertEquals("COMPLETED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentCompleted", "INVENTORY:StockReserved", "SHIPPING:ShipmentScheduled", "ORDER:OrderCompleted"), businessEvents(id));
        assertEquals(List.of("SagaStarted", "CommandSent", "CommandSent", "CommandSent", "SagaCompleted"), orchestratorEvents(id));
        assertMoney("ana", 960);
        assertEquals(98, stock("MOUSE"));
        mvc.perform(get("/api/orders/" + id)).andExpect(status().isOk()).andExpect(jsonPath("$.sagaState").value("COMPLETED"));
    }

    @Test
    @DisplayName("ORQUESTACIÓN, falla el pago: la saga termina compensada sin deshacer nada")
    void orchestrationPaymentFails() throws Exception {
        String id = create("orchestration", "luis", "LAPTOP", 1, "Calle 1");
        assertEquals("CANCELLED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentFailed", "ORDER:OrderCancelled"), businessEvents(id));
        assertEquals("SagaCompensated", orchestratorEvents(id).get(orchestratorEvents(id).size() - 1));
        assertMoney("luis", 50);
        assertEquals("saldo insuficiente", jdbc.queryForObject("select reason from orders where id = ?", String.class, id));
    }

    @Test
    @DisplayName("ORQUESTACIÓN, falla el inventario: el orquestador ordena reembolsar")
    void orchestrationStockFails() throws Exception {
        String id = create("orchestration", "ana", "SCARCE", 6, "Calle 1");
        assertEquals("CANCELLED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentCompleted", "INVENTORY:StockRejected", "PAYMENT:PaymentRefunded", "ORDER:OrderCancelled"), businessEvents(id));
        assertTrue(orchestratorEvents(id).containsAll(List.of("StepFailed", "CompensationStarted", "SagaCompensated")));
        assertMoney("ana", 1000);
        assertEquals(5, stock("SCARCE"));
    }

    @Test
    @DisplayName("ORQUESTACIÓN, falla el envío: compensa en orden inverso (liberar stock, luego reembolsar)")
    void orchestrationShippingFails() throws Exception {
        String id = create("orchestration", "ana", "LAPTOP", 1, "INVALID address");
        assertEquals("CANCELLED", awaitFinished(id));

        assertEquals(List.of("ORDER:OrderCreated", "PAYMENT:PaymentCompleted", "INVENTORY:StockReserved", "SHIPPING:ShipmentFailed", "INVENTORY:StockReleased",
                "PAYMENT:PaymentRefunded", "ORDER:OrderCancelled"), businessEvents(id));
        assertMoney("ana", 1000);
        assertEquals(10, stock("LAPTOP"));
    }

    @Test
    @DisplayName("ORQUESTACIÓN con un servicio caído: se agota el tiempo, se compensa, y al volver la compensación llega DESPUÉS del paso rezagado")
    void orchestrationTimeoutAndLateCommand() throws Exception {
        pause("inventory");
        String id = create("orchestration", "ana", "MOUSE", 3, "Calle 1");

        // El orquestador se impacienta: tras el tiempo de espera reembolsa el pago (inventario no ha respondido)
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> orchestratorEvents(id).contains("StepTimedOut"));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> businessEvents(id).contains("PAYMENT:PaymentRefunded") || orchestratorEvents(id).contains("CompensationStarted"));
        assertEquals("PENDING", statusOf(id), "la saga sigue COMPENSATING: falta la confirmación de inventario");
        assertEquals("COMPENSATING", jdbc.queryForObject("select state from sagas where order_id = ?", String.class, id));

        // El servicio vuelve: procesa ReserveStock (rezagado) y enseguida ReleaseStock (la compensación). Mismo tema, misma clave => mismo orden.
        registry.getListenerContainer("orch-inventory").resume();
        assertEquals("CANCELLED", awaitFinished(id));

        assertEquals(100, stock("MOUSE"), "el stock reservado por el comando rezagado se liberó");
        assertMoney("ana", 1000);
        assertEquals("COMPENSATED", jdbc.queryForObject("select state from sagas where order_id = ?", String.class, id));
        List<String> business = businessEvents(id);
        assertTrue(business.indexOf("INVENTORY:StockReserved") < business.indexOf("INVENTORY:StockReleased"), business.toString());
    }

    // ================================================================== INVARIANTES COMUNES
    @Test
    @DisplayName("CONCURRENCIA: 12 pedidos simultáneos de un producto con 5 en stock -> exactamente 5 se completan, 7 se cancelan y el dinero cuadra (ambos enfoques)")
    void concurrentOrdersKeepInvariants() throws Exception {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            ids.add(create(i % 2 == 0 ? "orchestration" : "choreography", "rich", "SCARCE", 1, "Calle " + i));
        }
        for (String id : ids) {
            awaitFinished(id);
        }

        long completed = ids.stream().filter(id -> statusOf(id).equals("COMPLETED")).count();
        assertEquals(5, completed, "solo había 5 unidades");
        assertEquals(7, ids.stream().filter(id -> statusOf(id).equals("CANCELLED")).count());
        assertEquals(0, stock("SCARCE"), "el stock nunca baja de 0");
        assertMoney("rich", 1_000_000 - 5 * 10);
        assertEquals(5, jdbc.queryForObject("select count(*) from shipments where status = 'SCHEDULED'", Integer.class));
        assertEquals(12, jdbc.queryForObject("select count(*) from payments", Integer.class), "se cobraron 12 y se reembolsaron 7");
        assertEquals(7, jdbc.queryForObject("select count(*) from payments where status = 'REFUNDED'", Integer.class));
    }

    @Test
    @DisplayName("IDEMPOTENCIA: el mismo mensaje entregado varias veces no cobra dos veces ni duplica efectos")
    void duplicateMessagesAreHarmless() throws Exception {
        String id = create("choreography", "ana", "MOUSE", 1, "Calle 1");
        assertEquals("COMPLETED", awaitFinished(id));
        assertMoney("ana", 980);

        // Kafka entrega "al menos una vez": se simulan repeticiones del primer evento y de uno intermedio
        SagaMessage created = new SagaMessage("OrderCreated", id, "ana", "MOUSE", 1, BigDecimal.valueOf(20), "Calle 1", null);
        bus.send(Topics.CHOREO_ORDER, id, created);
        bus.send(Topics.CHOREO_ORDER, id, created);
        bus.send(Topics.CHOREO_PAYMENT, id, created.as("PaymentCompleted"));
        Thread.sleep(2_000);

        assertEquals("COMPLETED", statusOf(id));
        assertMoney("ana", 980);
        assertEquals(99, stock("MOUSE"));
        assertEquals(1, jdbc.queryForObject("select count(*) from payments where order_id = ?", Integer.class, id));
        assertEquals(1, sagaLog.of(id).stream().filter(e -> e.event().equals("PaymentCompleted")).count(), "el cobro se registró una sola vez");
    }

    @Test
    @DisplayName("COMPENSACIÓN ANTES QUE LA ACCIÓN: un reembolso que llega antes que el cobro deja una 'lápida' y el cobro tardío se rechaza")
    void tombstoneRejectsALateAction() throws Exception {
        String id = "ord-tombstone";
        jdbc.update("insert into orders (id, mode, customer, sku, quantity, amount, address, status) values (?, 'CHOREOGRAPHY', 'ana', 'MOUSE', 1, 20, 'x', 'PENDING')", id);
        SagaMessage refund = new SagaMessage("StockRejected", id, "ana", "MOUSE", 1, BigDecimal.valueOf(20), "x", "prueba");
        bus.send(Topics.CHOREO_INVENTORY, id, refund);                       // el reembolso llega primero...
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> statusOf(id).equals("CANCELLED"));

        bus.send(Topics.CHOREO_ORDER, id, refund.as("OrderCreated"));        // ...y el cobro rezagado después
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> sagaLog.of(id).stream().anyMatch(e -> e.event().equals("LateChargeRejected")));

        assertMoney("ana", 1000);
        assertEquals("REFUNDED", jdbc.queryForObject("select status from payments where order_id = ?", String.class, id));
    }

    @Test
    @DisplayName("ORQUESTADOR: una respuesta duplicada o tardía se ignora y no altera la saga")
    void staleRepliesAreIgnored() throws Exception {
        String id = create("orchestration", "ana", "MOUSE", 1, "Calle 1");
        assertEquals("COMPLETED", awaitFinished(id));

        bus.send(Topics.ORCH_REPLIES, id, new SagaReply(id, "PAYMENT", "EXECUTE", false, "respuesta falsa"));   // fallo de un paso ya terminado
        bus.send(Topics.ORCH_REPLIES, id, new SagaReply(id, "INVENTORY", "EXECUTE", true, null));
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> orchestratorEvents(id).contains("ReplyIgnored"));

        assertEquals("COMPLETED", statusOf(id));
        assertEquals("COMPLETED", jdbc.queryForObject("select state from sagas where order_id = ?", String.class, id));
        assertMoney("ana", 980);
    }

    @Test
    @DisplayName("MENSAJE VENENO: un mensaje ilegible va a la cola de cartas muertas y no bloquea los siguientes")
    void poisonMessageGoesToTheDeadLetterTopic() throws Exception {
        String poisonId = "ord-poison-" + UUID.randomUUID();
        bus.send(Topics.CHOREO_ORDER, poisonId, Map.of("esto", "no es un mensaje de saga válido", "type", 12345));
        kafkaSendRaw(Topics.CHOREO_ORDER, poisonId, "{{{ esto ni siquiera es JSON");

        String id = create("choreography", "ana", "MOUSE", 1, "Calle 1");   // el flujo normal sigue funcionando detrás del veneno
        assertEquals("COMPLETED", awaitFinished(id));

        List<ConsumerRecord<String, String>> dead = readTopic(Topics.CHOREO_ORDER + ".DLT", poisonId);
        assertTrue(dead.stream().anyMatch(r -> r.value().contains("ni siquiera es JSON")), "el mensaje ilegible quedó en la DLT");
    }

    // ================================================================== API
    @Test
    @DisplayName("API: validaciones, producto inexistente, pedido inexistente y consulta con bitácora")
    void apiBehaviour() throws Exception {
        mvc.perform(post("/api/orchestration/orders").contentType(MediaType.APPLICATION_JSON).content("{\"customer\":\"\",\"sku\":\"\",\"quantity\":0}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.length()").value(3));
        mvc.perform(post("/api/choreography/orders").contentType(MediaType.APPLICATION_JSON).content("{\"customer\":\"ana\",\"sku\":\"NO-EXISTE\",\"quantity\":1,\"address\":\"x\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/orders/no-existe")).andExpect(status().isNotFound());
        mvc.perform(post("/api/admin/services/inventario/pause")).andExpect(status().isBadRequest());

        String id = create("orchestration", "ana", "MOUSE", 1, "Calle 1");
        awaitFinished(id);
        mvc.perform(get("/api/orders/" + id)).andExpect(status().isOk()).andExpect(jsonPath("$.log.length()").value(org.hamcrest.Matchers.greaterThan(5)))
                .andExpect(jsonPath("$.order.mode").value("ORCHESTRATION"));
        mvc.perform(get("/api/orders")).andExpect(status().isOk()).andExpect(jsonPath("$[0].id").value(id));
        mvc.perform(get("/api/admin/state")).andExpect(status().isOk()).andExpect(jsonPath("$.stock.length()").value(3));
    }

    // ------------------------------------------------------------------ utilidades de Kafka
    private void kafkaSendRaw(String topic, String key, String value) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props)) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        }
    }

    private List<ConsumerRecord<String, String>> readTopic(String topic, String key) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long end = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < end && found.stream().noneMatch(r -> r.value().contains("ni siquiera es JSON"))) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (key.equals(r.key())) {
                        found.add(r);
                    }
                });
            }
        }
        return found;
    }
}
