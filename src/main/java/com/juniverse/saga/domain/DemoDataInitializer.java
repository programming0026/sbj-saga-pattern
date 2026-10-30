package com.juniverse.saga.domain;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Carga los datos de demostración al arrancar. */
@Component
public class DemoDataInitializer implements ApplicationRunner {

    private final DemoData demoData;

    /**
     * Crea el inicializador.
     *
     * @param demoData datos de demostración
     */
    public DemoDataInitializer(DemoData demoData) {
        this.demoData = demoData;
    }

    @Override
    public void run(ApplicationArguments args) {
        demoData.reset();
    }
}
