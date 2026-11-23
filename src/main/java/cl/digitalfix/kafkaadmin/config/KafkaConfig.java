package cl.digitalfix.kafkaadmin.config;

import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    @Bean(destroyMethod = "close")
    Admin kafkaAdmin(@Value("${digitalfix.kafka.bootstrap-servers}") String brokers) {
        // El AdminClient gestiona metadatos y configuraciones; no consume eventos de negocio.
        return Admin.create(Map.of("bootstrap.servers", brokers, "client.id", "digitalfix-kafka-admin",
                "request.timeout.ms", 3000, "default.api.timeout.ms", 5000));
    }
}
