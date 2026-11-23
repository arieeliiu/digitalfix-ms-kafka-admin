package cl.digitalfix.kafkaadmin.controller;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice
public class Errores {
    @ExceptionHandler({org.springframework.web.bind.MethodArgumentNotValidException.class,
            org.springframework.http.converter.HttpMessageNotReadableException.class})
    ProblemDetail datosInvalidos(Exception error) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Revisa el formato y los parámetros de la solicitud");
    }
    @ExceptionHandler(ResponseStatusException.class)
    ProblemDetail solicitud(ResponseStatusException error) {
        return ProblemDetail.forStatusAndDetail(error.getStatusCode(), error.getReason());
    }
    @ExceptionHandler(KafkaException.class)
    ProblemDetail kafka(KafkaException error) {
        HttpStatus status = error instanceof UnknownTopicOrPartitionException ? HttpStatus.NOT_FOUND
                : error instanceof TopicExistsException ? HttpStatus.CONFLICT
                : error instanceof InvalidTopicException || error instanceof InvalidConfigurationException
                    || error instanceof InvalidReplicationFactorException || error instanceof InvalidPartitionsException ? HttpStatus.BAD_REQUEST
                : error instanceof TimeoutException ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.BAD_GATEWAY;
        return ProblemDetail.forStatusAndDetail(status, switch (status) {
            case NOT_FOUND -> "Tópico no encontrado";
            case CONFLICT -> "El tópico ya existe";
            case BAD_REQUEST -> "Parámetros rechazados por Kafka";
            case SERVICE_UNAVAILABLE -> "Kafka temporalmente no disponible";
            default -> "Kafka no pudo completar la operación";
        });
    }
}
