package com.juniverse.saga.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

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
