package com.juniverse.saga.domain;

/**
 * Resultado de una operación de un servicio.
 *
 * @param success si se pudo realizar
 * @param reason motivo cuando no se pudo
 */
public record Outcome(boolean success, String reason) {

    /** Operación correcta. */
    public static final Outcome OK = new Outcome(true, null);

    /**
     * Operación rechazada.
     *
     * @param reason motivo
     * @return resultado de fallo
     */
    public static Outcome failed(String reason) {
        return new Outcome(false, reason);
    }
}
