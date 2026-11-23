package cl.digitalfix.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

public record TopicoResponse(String nombre, List<Particion> particiones, Map<String, String> configuracion) {
    public record Particion(int particion, Integer lider, List<Integer> replicas, List<Integer> isr) {}
}
