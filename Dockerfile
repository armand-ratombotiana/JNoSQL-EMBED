FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/jnosql-embed-core-1.0.0.jar jnosql-embed.jar
RUN mkdir -p /data /logs
VOLUME ["/data"]
EXPOSE 8080 8081

ENV DATA_DIR=/data
ENV PORT=8080
ENV ENGINE=FILE
ENV FLUSH_MODE=sync
ENV FLUSH_INTERVAL=1000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:${PORT}/api/health || exit 1

ENTRYPOINT ["sh", "-c", "java -jar jnosql-embed.jar --port ${PORT} --data-dir ${DATA_DIR} --engine ${ENGINE} --flush-interval ${FLUSH_INTERVAL}"]
