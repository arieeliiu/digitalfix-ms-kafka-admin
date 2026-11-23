# DigitalFix Kafka Admin

Microservicio Java 21 / Spring Boot para administrar tópicos con Kafka AdminClient.
No consume eventos ni sustituye Rabbit Admin. Implementa
[Infra #13](https://github.com/arieeliiu/digitalfix-infra/issues/13).

## Configuración y seguridad

Exportar las variables de `.env.example`; Spring no carga `.env` automáticamente.
`KAFKA_BOOTSTRAP_SERVERS`: localhost:9092/9093/9094 desde el host;
kafka1:29092,kafka2:29092,kafka3:29092 desde la red Docker compartida.
`ENTRA_ISSUER` y `ENTRA_AUDIENCE` son obligatorias. Puerto 8086 por defecto,
configurable con `SERVER_PORT`. Ejecutar `.\mvnw.cmd spring-boot:run`.

La API administrativa exige access token válido para el issuer/audience configurados,
scope `access_as_user` y rol **Admin**. Auditor, Operador y Cliente reciben 403.
La autorización HTTP protege esta API; los brokers deben permanecer en red privada.
Esta configuración no añade ACL/SASL al clúster.

## API

Enviar `Authorization: Bearer <access-token>` y Content-Type application/json.

| Método/ruta | Respuesta |
|---|---|
| GET `/api/kafka/topics` | Nombres ordenados, excluye internos |
| POST `/api/kafka/topics` | 201; crea tópico |
| GET `/api/kafka/topics/{nombre}` | Particiones, líder, réplicas, ISR y configuraciones |
| DELETE `/api/kafka/topics/{nombre}` | 204; elimina tópico y sus registros |
| GET `/api/kafka/topics/{nombre}/config` | Retención, cleanup y minISR efectivos |
| PATCH `/api/kafka/topics/{nombre}/config` | 204; modifica solo las claves enviadas |
| GET `/healthz` | Público; comprueba HTTP, no disponibilidad Kafka |

Crear un tópico de demostración:

```json
{
  "nombre": "demo-digitalfix",
  "particiones": 3,
  "replicas": 3,
  "configuracion": {
    "retention.ms": "60000",
    "min.insync.replicas": "2",
    "cleanup.policy": "delete"
  }
}
```

PATCH `/api/kafka/topics/demo-digitalfix/config`:

```json
{"configuracion":{"retention.ms":"120000"}}
```

Nombre: letras, números, punto, guion y guion bajo, máximo 249; rechaza vacío,
`.`/`..` e internos `__*`. Particiones 1..10000 y réplicas 1..32767; Kafka valida
además la capacidad del clúster. Configuraciones admitidas: `retention.ms` (entero
>= -1), `min.insync.replicas` (1..RF), `cleanup.policy` (`delete`, `compact` o ambas).
IncrementalAlterConfigs conserva las claves omitidas. Cambiar retención/cleanup
puede afectar datos existentes: mantener el contrato de los tópicos de negocio.

Errores: 400 parámetros inválidos, 401 token ausente/inválido, 403 sin scope/rol,
404 tópico inexistente, 409 nombre existente, 503 timeout, 502 otro fallo Kafka.
No devuelve excepciones internas. Las operaciones esperan confirmación; un timeout
puede dejar resultado incierto: consultar el estado antes de repetir escrituras.

## Pruebas y Docker

```powershell
.\mvnw.cmd test
.\mvnw.cmd '-Ddigitalfix.pruebas.kafka=true' test
docker build -t digitalfix/kafka-admin:local .
```

Pruebas normales: siete casos de validación y una integración omitida. La prueba
optativa requiere tres brokers locales y Kafka UI `http://localhost:8090`; pasaron
los ocho casos. Admite `-Ddigitalfix.pruebas.kafka.bootstrap=localhost:9092,localhost:9093,localhost:9094`
y `-Ddigitalfix.pruebas.kafka.ui=http://localhost:8090/api/clusters/digitalfix/topics`.
Solo acepta destinos loopback.

También pasó el mismo CRUD dentro de la imagen Docker, conectada al listener
interno `29092`. Para repetirlo en Docker Desktop, tras construir la imagen:

```powershell
.\mvnw.cmd '-Dtest=KafkaAdminRealTests' '-Ddigitalfix.pruebas.kafka=true' '-Ddigitalfix.pruebas.docker=true' test
```

La red predeterminada es `digitalfix-kafka-verificacion_kafkanet`; se puede indicar
otra con `-Ddigitalfix.pruebas.docker.red=NOMBRE`. Esta variante publica la clave
JWKS pública durante la prueba para que el contenedor acceda por
`host.docker.internal`. El contenedor aleatorio y el servidor JWKS se retiran al
terminar; no instala claves de prueba en la imagen ni en la configuración normal.

La integración usa HTTP y JWT RSA/JWKS reales locales, sin AdminClient simulado.
Crea un tópico aleatorio `validacion-admin-*`, comprueba 3 particiones/RF3/ISR3,
configura retención conservando minISR/cleanup, lo encuentra en la API de Kafka UI
y elimina solo ese tópico. Verifica respuestas 400/401/403/404/409. Falta repetir
el acceso con tokens Entra reales y en el despliegue coordinado.

Docker usa un runtime sin privilegios. Infra incluye `apps/compose.kafka-admin.yml`
para arrancar solo este administrador sobre la red Kafka externa, con 8086 en
loopback, sin iniciar aplicaciones Oracle. La demo debe usar un tópico temporal;
no eliminar `workorders.events`, `audit.timeline` ni sus DLT.
