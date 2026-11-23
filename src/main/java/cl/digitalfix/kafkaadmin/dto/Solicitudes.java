package cl.digitalfix.kafkaadmin.dto;

import java.util.Map;
import jakarta.validation.constraints.*;

public final class Solicitudes {
    private Solicitudes() {}
    public record CrearTopico(@NotBlank String nombre, @NotNull @Min(1) @Max(10000) Integer particiones,
            @NotNull @Min(1) @Max(32767) Integer replicas, Map<String, String> configuracion) {}
    public record ActualizarConfig(@NotEmpty Map<String, String> configuracion) {}
}
