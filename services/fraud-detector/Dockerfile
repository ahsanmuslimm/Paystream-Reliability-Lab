# PayStream Reliability Lab - fraud-detector image (multi-stage, non-root)
# Build context is services/ so the multi-module parent POM resolves.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml ./
COPY common-avro/pom.xml common-avro/
COPY fraud-detector/pom.xml fraud-detector/
COPY common-avro/src common-avro/src
COPY fraud-detector/src fraud-detector/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl common-avro,fraud-detector -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
COPY --from=build /build/fraud-detector/target/fraud-detector-*.jar app.jar
ENV JAVA_OPTS="-Xmx512m"
EXPOSE 8082
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
