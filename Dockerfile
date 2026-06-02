# --- build stage ---
FROM gradle:8.10.2-jdk21 AS build
WORKDIR /workspace

# Cache dependency resolution: copy build scripts first, then sources.
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
# Strip machine-specific JDK paths — the build image provides JDK 21 as its default toolchain.
RUN sed -i '/org.gradle.java/d' gradle.properties
COPY scheduler-core/build.gradle.kts scheduler-core/
COPY worker/build.gradle.kts         worker/
COPY api/build.gradle.kts            api/
COPY raft/build.gradle.kts           raft/
RUN gradle :api:dependencies --no-daemon > /dev/null 2>&1 || true

COPY . .
RUN sed -i '/org.gradle.java/d' gradle.properties
RUN gradle :api:bootJar --no-daemon -x test

# --- runtime stage ---
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

RUN useradd --system --uid 1001 kinetis
USER kinetis

COPY --from=build /workspace/api/build/libs/*.jar /app/app.jar

EXPOSE 8080
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseZGC"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
