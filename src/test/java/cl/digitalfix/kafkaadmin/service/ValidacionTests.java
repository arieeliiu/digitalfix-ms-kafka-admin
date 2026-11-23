package cl.digitalfix.kafkaadmin.service;

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;
import static org.junit.jupiter.api.Assertions.*;

class ValidacionTests {
    @ParameterizedTest @ValueSource(strings = {"", ".", "..", "__consumer_offsets", "con espacios", "ruta/invalida"})
    void rechazaNombresInvalidos(String nombre) {
        assertThrows(ResponseStatusException.class, () -> TopicosService.validarNombre(nombre));
    }
    @Test void validaValoresYNoAdmiteConfiguracionesArbitrarias() {
        for (var config : java.util.List.of(Map.of("retention.ms", "-2"), Map.of("retention.ms", "abc"),
                Map.of("min.insync.replicas", "4"), Map.of("cleanup.policy", "otra"), Map.of("segment.bytes", "123")))
            assertThrows(ResponseStatusException.class, () -> TopicosService.validarConfig(config, 3));
        TopicosService.validarNombre("demo-digitalfix.1");
        TopicosService.validarConfig(Map.of("retention.ms", "-1", "min.insync.replicas", "2", "cleanup.policy", "delete"), 3);
    }
}
