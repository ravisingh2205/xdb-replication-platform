# Builds any backend module:  docker build -f docker/service.Dockerfile --build-arg MODULE=apply-service .
# ---- build ----
FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/
COPY api-gateway/pom.xml api-gateway/
COPY publication-service/pom.xml publication-service/
COPY subscription-service/pom.xml subscription-service/
COPY apply-service/pom.xml apply-service/
# Dependency layer, cached until a pom changes.
RUN mvn -q -B -pl ${MODULE} -am dependency:go-offline -DskipTests || true
COPY . .
RUN mvn -q -B -pl ${MODULE} -am package -DskipTests && cp ${MODULE}/target/${MODULE}-*.jar /app.jar

# ---- run ----
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S xdb && adduser -S xdb -G xdb
USER xdb
COPY --from=build /app.jar /app/app.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080 8081 8082 8084
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
