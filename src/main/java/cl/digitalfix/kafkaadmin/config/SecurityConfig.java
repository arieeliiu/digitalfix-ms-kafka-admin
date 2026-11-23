package cl.digitalfix.kafkaadmin.config;

import java.util.ArrayList;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;
import static org.springframework.security.authorization.AuthorityAuthorizationManager.hasAuthority;
import static org.springframework.security.authorization.AuthorizationManagers.allOf;

@Configuration
public class SecurityConfig {
    @Bean
    SecurityFilterChain seguridad(HttpSecurity http) throws Exception {
        var scopes = new JwtGrantedAuthoritiesConverter();
        var roles = new JwtGrantedAuthoritiesConverter();
        roles.setAuthoritiesClaimName("roles"); roles.setAuthorityPrefix("ROLE_");
        var converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(jwt -> {
            var authorities = new ArrayList<GrantedAuthority>(scopes.convert(jwt));
            authorities.addAll(roles.convert(jwt)); return authorities;
        });
        return http.csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(r -> r.requestMatchers(HttpMethod.GET, "/healthz").permitAll()
                    // Firma, emisor y audiencia se validan antes de revisar scope y rol.
                    .requestMatchers("/api/kafka/**").access(allOf(hasAuthority("SCOPE_access_as_user"), hasAuthority("ROLE_Admin")))
                    .anyRequest().denyAll())
                .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(converter))).build();
    }
}
