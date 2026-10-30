package com.juniverse.saga.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Reloj inyectable. */
@Configuration
public class ClockConfig {

    /**
     * Reloj del sistema en UTC.
     *
     * @return reloj
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
