package cl.digitalfix.kafkaadmin;

import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import com.sun.net.httpserver.HttpServer;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import static org.junit.jupiter.api.Assertions.*;

// Broker y HTTP reales. Solo modifica un tópico aleatorio que la propia prueba crea.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnabledIfSystemProperty(named = "digitalfix.pruebas.kafka", matches = "true")
class KafkaAdminRealTests {
    static final RSAKey CLAVE = clave();
    static final HttpServer JWKS = jwks();
    static final String ISSUER = "http://127.0.0.1:" + JWKS.getAddress().getPort();
    @Value("${local.server.port}") int puerto;
    static String contenedor;
    int puertoDocker;
    static final tools.jackson.databind.json.JsonMapper JSON = tools.jackson.databind.json.JsonMapper.builder().build();

    @DynamicPropertySource static void propiedades(DynamicPropertyRegistry props) {
        String brokers = System.getProperty("digitalfix.pruebas.kafka.bootstrap", "localhost:9092,localhost:9093,localhost:9094");
        for (String broker : brokers.split(",")) if (!broker.matches("(localhost|127\\.0\\.0\\.1):[0-9]+"))
            throw new IllegalArgumentException("Prueba habilitada solo para brokers loopback");
        props.add("digitalfix.kafka.bootstrap-servers", () -> brokers);
        props.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> ISSUER);
        props.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", () -> ISSUER + "/claves");
        props.add("spring.security.oauth2.resourceserver.jwt.audiences", () -> "pruebas-locales");
    }
    @Test void administraTopicoRealConJwtYSeVeEnKafkaUi() throws Exception {
        if (Boolean.getBoolean("digitalfix.pruebas.docker")) iniciarDocker();
        String nombre = "validacion-admin-" + UUID.randomUUID();
        String ruta = "/api/kafka/topics/" + nombre;
        try (var http = HttpClient.newHttpClient()) {
            assertEquals(200, pedir(http, "GET", "/healthz", null, null).statusCode());
            assertEquals(401, pedir(http, "GET", "/api/kafka/topics", null, null).statusCode());
            for (String rol : List.of("Auditor", "Operador", "Cliente"))
                for (String metodo : List.of("GET", "POST", "PATCH", "DELETE"))
                    assertEquals(403, pedir(http, metodo, metodo.equals("POST") ? "/api/kafka/topics" : metodo.equals("PATCH") ? ruta + "/config" : ruta,
                            token(rol, true), metodo.equals("POST") || metodo.equals("PATCH") ? "{}" : null).statusCode());
            assertEquals(403, pedir(http, "GET", "/api/kafka/topics", token("Admin", false), null).statusCode());
            String invalido = token("Admin", true);
            int inicio = invalido.lastIndexOf('.') + 1;
            invalido = invalido.substring(0, inicio) + (invalido.charAt(inicio) == 'A' ? 'B' : 'A') + invalido.substring(inicio + 1);
            assertEquals(401, pedir(http, "GET", "/api/kafka/topics", invalido, null).statusCode());
            String jwt = token("Admin", true);
            String crear = JSON.writeValueAsString(Map.of("nombre", nombre, "particiones", 3, "replicas", 3,
                    "configuracion", Map.of("retention.ms", "60000", "min.insync.replicas", "2", "cleanup.policy", "delete")));
            assertEquals(400, pedir(http, "POST", "/api/kafka/topics", jwt, "{\"nombre\":\"__interno\",\"particiones\":3,\"replicas\":3}").statusCode());
            assertEquals(400, pedir(http, "POST", "/api/kafka/topics", jwt, "{\"nombre\":\"invalido\",\"particiones\":0,\"replicas\":3}").statusCode());
            boolean creado = false;
            try {
                var respuesta = pedir(http, "POST", "/api/kafka/topics", jwt, crear);
                assertEquals(201, respuesta.statusCode(), respuesta.body()); creado = true;
                assertEquals(409, pedir(http, "POST", "/api/kafka/topics", jwt, crear).statusCode());
                assertTrue(pedir(http, "GET", "/api/kafka/topics", jwt, null).body().contains(nombre));
                var topico = JSON.readTree(pedir(http, "GET", ruta, jwt, null).body());
                assertEquals(3, topico.get("particiones").size());
                for (var p : topico.get("particiones")) { assertEquals(3, p.get("replicas").size()); assertEquals(3, p.get("isr").size()); }
                assertEquals(204, pedir(http, "PATCH", ruta + "/config", jwt, "{\"configuracion\":{\"retention.ms\":\"120000\"}}").statusCode());
                var config = JSON.readTree(pedir(http, "GET", ruta + "/config", jwt, null).body());
                assertEquals("120000", config.get("retention.ms").asText());
                assertEquals("2", config.get("min.insync.replicas").asText());
                assertEquals("delete", config.get("cleanup.policy").asText());
                assertEquals(400, pedir(http, "PATCH", ruta + "/config", jwt, "{\"configuracion\":{\"min.insync.replicas\":\"4\"}}").statusCode());
                String ui = System.getProperty("digitalfix.pruebas.kafka.ui", "http://localhost:8090/api/clusters/digitalfix/topics");
                URI uri = URI.create(ui);
                assertTrue(Set.of("localhost", "127.0.0.1").contains(uri.getHost()), "UI de prueba local");
                boolean visible = false;
                long limite = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (!visible && System.nanoTime() < limite) {
                    var vista = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
                    visible = vista.statusCode() == 200 && vista.body().contains(nombre);
                    if (!visible) Thread.sleep(500);
                }
                assertTrue(visible, "El tópico debe aparecer en la API de Kafka UI");
                System.out.println("KAFKA_ADMIN_REAL OK: HTTP CRUD, 3 particiones/RF3/ISR3, configuración incremental, JWT y visibilidad Kafka UI");
            } finally { if (creado) assertEquals(204, pedir(http, "DELETE", ruta, jwt, null).statusCode()); }
            assertEquals(404, pedir(http, "GET", ruta, jwt, null).statusCode());
            assertEquals(404, pedir(http, "DELETE", ruta, jwt, null).statusCode());
        }
    }
    HttpResponse<String> pedir(HttpClient http, String metodo, String ruta, String token, String body) throws Exception {
        int destino = puertoDocker == 0 ? puerto : puertoDocker;
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + destino + ruta)).timeout(Duration.ofSeconds(12));
        if (token != null) request.header("Authorization", "Bearer " + token);
        if (body != null) request.header("Content-Type", "application/json");
        return http.send(request.method(metodo, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    static String token(String rol, boolean scope) throws Exception {
        var ahora = Instant.now();
        var claims = new JWTClaimsSet.Builder().issuer(ISSUER).audience("pruebas-locales").subject("prueba-admin")
                .claim("roles", List.of(rol)).issueTime(Date.from(ahora)).expirationTime(Date.from(ahora.plusSeconds(300)));
        if (scope) claims.claim("scp", "access_as_user");
        var jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(CLAVE.getKeyID()).build(), claims.build());
        jwt.sign(new RSASSASigner(CLAVE)); return jwt.serialize();
    }
    static RSAKey clave() { try { return new RSAKeyGenerator(2048).keyID("prueba-local").generate(); } catch (Exception e) { throw new IllegalStateException(e); } }
    static HttpServer jwks() {
        try {
            // Solo en la prueba Docker, publicar la clave pública permite usar host.docker.internal.
            String host = Boolean.getBoolean("digitalfix.pruebas.docker") ? "0.0.0.0" : "127.0.0.1";
            var servidor = HttpServer.create(new InetSocketAddress(host, 0), 0);
            byte[] datos = new JWKSet(CLAVE.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
            servidor.createContext("/claves", x -> { x.getResponseHeaders().set("Content-Type", "application/json"); x.sendResponseHeaders(200, datos.length); try (var out = x.getResponseBody()) { out.write(datos); } });
            servidor.start(); return servidor;
        } catch (Exception e) { throw new IllegalStateException(e); }
    }
    void iniciarDocker() throws Exception {
        try (var socket = new java.net.ServerSocket(0, 0, InetAddress.getLoopbackAddress())) { puertoDocker = socket.getLocalPort(); }
        contenedor = "validacion-kafka-admin-" + UUID.randomUUID();
        String red = System.getProperty("digitalfix.pruebas.docker.red", "digitalfix-kafka-verificacion_kafkanet");
        var proceso = new ProcessBuilder("docker", "run", "--rm", "-d", "--name", contenedor,
                "--network", red, "-p", "127.0.0.1:" + puertoDocker + ":8086",
                "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka1:29092,kafka2:29092,kafka3:29092",
                "-e", "ENTRA_ISSUER=" + ISSUER, "-e", "ENTRA_AUDIENCE=pruebas-locales",
                "-e", "SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWK_SET_URI=http://host.docker.internal:" + JWKS.getAddress().getPort() + "/claves",
                "digitalfix/kafka-admin:local").redirectErrorStream(true).start();
        String salida = new String(proceso.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, proceso.waitFor(), salida);
        try (var http = HttpClient.newHttpClient()) {
            long limite = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (System.nanoTime() < limite) {
                try { if (pedir(http, "GET", "/healthz", null, null).statusCode() == 200) return; }
                catch (java.io.IOException ignored) {}
                Thread.sleep(300);
            }
        }
        fail("No arrancó Kafka Admin en Docker");
    }
    @AfterAll static void detener() throws Exception {
        JWKS.stop(0);
        if (contenedor != null) {
            var proceso = new ProcessBuilder("docker", "stop", contenedor).redirectErrorStream(true).start();
            proceso.getInputStream().readAllBytes(); proceso.waitFor();
        }
    }
}
