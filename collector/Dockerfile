FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre-jammy
RUN useradd --create-home --uid 10001 collector
WORKDIR /app
COPY --from=build /build/target/vk-collector.jar /app/vk-collector.jar
USER collector
ENTRYPOINT ["java", "-jar", "/app/vk-collector.jar"]
CMD ["--help"]
