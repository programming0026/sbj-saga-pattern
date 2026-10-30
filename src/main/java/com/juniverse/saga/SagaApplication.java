package com.juniverse.saga;

import com.juniverse.saga.config.SagaProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Patrón <b>Saga</b>: cómo mantener la consistencia de un proceso de negocio que atraviesa varios servicios, cada uno con su propia
 * base de datos, cuando NO se puede usar una transacción distribuida.
 *
 * <p>Una saga es una secuencia de <i>transacciones locales</i>. Si un paso falla, no hay "rollback" global: se ejecutan
 * <b>transacciones compensatorias</b> que deshacen los pasos ya hechos (reembolsar el pago, liberar el stock...).</p>
 *
 * <p>El ejemplo implementa el mismo proceso (pedido → pago → inventario → envío) de las dos formas clásicas, sobre Kafka:</p>
 * <ul>
 *   <li><b>Orquestación</b>: un orquestador central dice a cada servicio qué hacer (comandos) y decide el siguiente paso o la
 *       compensación según sus respuestas.</li>
 *   <li><b>Coreografía</b>: no hay jefe; cada servicio reacciona a los eventos de los demás y publica los suyos.</li>
 * </ul>
 * Para poder ejecutarlo con un solo comando, los "servicios" son paquetes de esta aplicación que SOLO se comunican por Kafka y tienen
 * sus propias tablas; separarlos en aplicaciones distintas no cambia el diseño.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(SagaProperties.class)
public class SagaApplication {

    /**
     * Arranca la aplicación.
     *
     * @param args argumentos de la línea de comandos
     */
    public static void main(String[] args) {
        SpringApplication.run(SagaApplication.class, args);
    }
}
