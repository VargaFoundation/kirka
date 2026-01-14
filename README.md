# Kirka

Kirka is an MLFlow compatible service optimized for the Hadoop ecosystem. It uses HDFS for artifact storage and HBase for metadata and metrics tracking.

## Features

- **MLFlow API Compatibility**: Supports standard MLFlow clients (Python, Java, R).
- **HBase Backend**: High-scalability tracking for experiments, runs, parameters, and metrics.
- **HDFS Artifact Storage**: Native storage for models, logs, and plots.
- **Kerberos Security**: Optional Kerberos authentication for secure Hadoop clusters.

## Project Structure

- `groupId`: `varga.foundation`
- `artifactId`: `kirka`
- `package`: `varga.kirka`

## Prerequisites

- Java 17
- Maven 3.6+
- Apache HBase 2.5.x
- Apache Hadoop 3.3.x
- (Optional) Kerberos KDC for secure clusters

## Building the Project

To build the project and package it into a JAR:

```bash
mvn clean package
```

The resulting JAR will be located in the `target/` directory.

## Configuration

Edit `src/main/resources/application.properties` to configure the service.

### HBase and HDFS

```properties
# HBase Configuration
hbase.zookeeper.quorum=your-zookeeper-host
hbase.zookeeper.property.clientPort=2181

# HDFS Configuration
hadoop.hdfs.uri=hdfs://your-namenode-host:9000
```

### Kerberos Security (Optional)

To enable Kerberos authentication:

```properties
security.kerberos.enabled=true
security.kerberos.principal=user@REALM.COM
security.kerberos.keytab=/path/to/user.keytab
security.kerberos.krb5conf=/etc/krb5.conf
```

## Apache Knox Integration

Kirka can be exposed through Apache Knox to provide a single point of entry, authentication, and service discovery.

### 1. Service Definition

Add the files in the `knox/` directory to your Knox installation:
- Copy `knox/service.xml` and `knox/rewrite.xml` to `{KNOX_HOME}/data/services/kirka/0.0.1/`

### 2. Topology Configuration

Add the following service to your Knox topology file (e.g., `default.xml`):

```xml
<service>
    <role>KIRKA</role>
    <url>http://{kirka-host}:{kirka-port}</url>
</service>
```

### 3. Accessing Kirka via Knox

Once configured, the MLFlow API can be accessed through Knox. Pour plus de détails sur la configuration du client MLFlow avec Knox, consultez le guide [Utilisation de MLFlow avec Kirka via Apache Knox](USAGE_MLFLOW_KNOX.md).

Exemple rapide avec le client Python MLFlow :
```python
import mlflow
# Remplacez par votre URL Knox réelle
mlflow.set_tracking_uri("https://knox-gateway:8443/gateway/sandbox/kirka")
```

## Running the Application

You can run the application using Maven:

```bash
mvn spring-boot:run
```

Or by running the JAR directly:

```bash
java -jar target/kirka-0.0.1-SNAPSHOT.jar
```

## Supported MLFlow API Routes

The service implements the following MLFlow REST API v2.0 routes:

### Experiments
- `POST /api/2.0/mlflow/experiments/create`
- `GET /api/2.0/mlflow/experiments/get`
- `GET /api/2.0/mlflow/experiments/get-by-name`
- `GET /api/2.0/mlflow/experiments/list`
- `GET /api/2.0/mlflow/experiments/search`
- `POST /api/2.0/mlflow/experiments/update`
- `POST /api/2.0/mlflow/experiments/delete`
- `POST /api/2.0/mlflow/experiments/restore`
- `POST /api/2.0/mlflow/experiments/set-experiment-tag`

### Runs
- `POST /api/2.0/mlflow/runs/create`
- `GET /api/2.0/mlflow/runs/get`
- `POST /api/2.0/mlflow/runs/update`
- `POST /api/2.0/mlflow/runs/delete`
- `POST /api/2.0/mlflow/runs/restore`
- `POST /api/2.0/mlflow/runs/search`
- `POST /api/2.0/mlflow/runs/log-parameter`
- `POST /api/2.0/mlflow/runs/log-metric`
- `POST /api/2.0/mlflow/runs/log-batch`
- `POST /api/2.0/mlflow/runs/set-tag`
- `POST /api/2.0/mlflow/runs/delete-tag`
- `GET /api/2.0/mlflow/runs/get-metric-history`

### Artifacts
- `GET /api/2.0/mlflow/artifacts/list`
- `POST /api/2.0/mlflow/artifacts/upload`
- `GET /api/2.0/mlflow/artifacts/download`

### Model Registry
- `POST /api/2.0/mlflow/registered-models/create`
- `GET /api/2.0/mlflow/registered-models/get`
- `GET /api/2.0/mlflow/registered-models/list`
- `GET /api/2.0/mlflow/registered-models/search`
- `POST /api/2.0/mlflow/registered-models/update`
- `POST /api/2.0/mlflow/registered-models/delete`
- `POST /api/2.0/mlflow/model-versions/create`
- `GET /api/2.0/mlflow/model-versions/get`
- `POST /api/2.0/mlflow/model-versions/update`
- `POST /api/2.0/mlflow/model-versions/delete`
- `POST /api/2.0/mlflow/model-versions/transition-stage`

## License

This project is licensed under the MIT License - see the LICENSE file for details.
