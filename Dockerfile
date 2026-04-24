# syntax=docker/dockerfile:1.6

# ---- Dependency resolution (cacheable) ----
FROM maven:3.9-eclipse-temurin-17 AS deps
WORKDIR /build
COPY pom.xml .
# Resolve all dependencies and plugins into the local cache so later changes to src/
# don't re-trigger network fetches when the layer cache hits.
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests dependency:go-offline

# ---- Build ----
FROM deps AS build
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests package && \
    cp target/kirka-*.jar /tmp/app.jar

# ---- Runtime ----
FROM eclipse-temurin:17-jre-jammy

ARG APP_USER=kirka
ARG APP_UID=10001
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system --gid ${APP_UID} ${APP_USER} && \
    useradd --system --uid ${APP_UID} --gid ${APP_USER} --no-create-home --shell /sbin/nologin ${APP_USER}

WORKDIR /app
COPY --from=build --chown=${APP_UID}:${APP_UID} /tmp/app.jar /app/app.jar

USER ${APP_UID}

EXPOSE 8080

# Liveness + crash-on-OOM make Kubernetes / Docker observe real failure states instead of
# silently looping on a stuck JVM.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl --fail --silent --show-error http://localhost:8080/actuator/health/liveness || exit 1

ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -XX:+ExitOnOutOfMemoryError -jar /app/app.jar"]
