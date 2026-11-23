FROM maven:3.9-eclipse-temurin-21 AS compilacion
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN sed -i 's/\r$//' mvnw && chmod +x mvnw
COPY src/ src/
RUN mvn -B -DskipTests package

FROM compilacion AS pruebas
ENTRYPOINT ["./mvnw", "-B", "test"]

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S digitalfix && adduser -S digitalfix -G digitalfix
WORKDIR /app
COPY --from=compilacion /app/target/*.jar app.jar
USER digitalfix
EXPOSE 8086
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "app.jar"]
