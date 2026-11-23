package cl.digitalfix.kafkaadmin.controller;

import java.util.*;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import cl.digitalfix.kafkaadmin.dto.Solicitudes.*;
import cl.digitalfix.kafkaadmin.dto.TopicoResponse;
import cl.digitalfix.kafkaadmin.service.TopicosService;

@RestController
public class TopicosController {
    private final TopicosService servicio;
    public TopicosController(TopicosService servicio) { this.servicio = servicio; }
    @GetMapping("/healthz")
    public Map<String, String> salud() { return Map.of("status", "UP"); }
    @GetMapping("/api/kafka/topics")
    public List<String> listar() { return servicio.listar(); }
    @PostMapping("/api/kafka/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public void crear(@Valid @RequestBody CrearTopico solicitud) { servicio.crear(solicitud); }
    @GetMapping("/api/kafka/topics/{nombre}")
    public TopicoResponse describir(@PathVariable String nombre) { return servicio.describir(nombre); }
    @DeleteMapping("/api/kafka/topics/{nombre}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void eliminar(@PathVariable String nombre) { servicio.eliminar(nombre); }
    @GetMapping("/api/kafka/topics/{nombre}/config")
    public Map<String, String> configuracion(@PathVariable String nombre) { return servicio.configuracion(nombre); }
    @PatchMapping("/api/kafka/topics/{nombre}/config")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void actualizar(@PathVariable String nombre, @Valid @RequestBody ActualizarConfig solicitud) {
        servicio.actualizar(nombre, solicitud.configuracion());
    }
}
