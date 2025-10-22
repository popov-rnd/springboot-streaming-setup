# 🧩 Popov R&D — Spring Boot Kafka Example

This repository demonstrates a **multi-module Maven project** built with **Spring Boot 3.x** to showcase reliable, observable streaming with **Apache Kafka**.

It includes two independent applications that communicate through a single Kafka broker:

| Module              | Description                                                                                                 |
|---------------------|-------------------------------------------------------------------------------------------------------------|
| 🚀 **producer** | Publishes `MyEvent` events with full reliability (idempotence, transactions, retries).                      |
| 📡 **consumer** | Listens for events, processes them, and supports manual/batch commits and Dead-Letter Topic (DLT) handling. |

Producers own **topics** — they publish into them.  
Consumers own **consumer groups** — they subscribe from them.

---

Each submodule can be run separately but shares common settings and version alignment via the parent POM.

---

## 🧰 Prerequisites

- **JDK 21+**
- **Maven 3.9+**
- **Docker** — required to run Kafka locally
- **Kafka image:** `apache/kafka:latest` (KRaft mode, no Zookeeper required)

---

## 🪣 Start Kafka with Docker Compose

For local development, use the included `docker-compose.yml`:

```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      # --- KRaft mode (no Zookeeper) ---
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      # --- Broker configuration ---
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
      # --- Storage ---
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka_data:/var/lib/kafka/data
    restart: unless-stopped

volumes:
  kafka_data:
```

## 🚀 Launch Applications

```
# From project root
./mvnw clean package

# Start producer
./mvnw -pl producer spring-boot:run

# Start consumer
./mvnw -pl consumer spring-boot:run
```

## 🚀 Producer Module

This module demonstrates a **Spring Boot 3.x Kafka producer** that sends events with **delivery guarantees**, **observability**, and **fallback recovery**.

---

### ⚙️ Features

- ✅ **Asynchronous Publishing** — Sends messages using `KafkaTemplate.send()` with completion callbacks for success or failure.
- ✅ **JSON Serialization** — Serializes `MyEvent` objects to JSON via `JsonSerializer` (JSON is for demo only, not prod!).
- ✅ **Topic Auto-Configuration** — Defines a `NewTopic` bean in the producer module (Local development, not prod!). Spring Boot detects it and uses `KafkaAdmin` to automatically create the topic on startup if it doesn’t exist (idempotent).
- ✅ **Durable Delivery** — Uses `acks=all` so all in-sync replicas acknowledge each message before confirming success.
- ✅ **Automatic Retries** — Retries transient network or leader-election errors up to `3` times, fully handled by the Kafka client.
- ✅ **Performance Tuning** — Kafka client batches messages with `linger.ms=5 ms` and compresses payloads using `gzip` to boost throughput.
- ✅ **Observability** — low-level retry and network logs available at `org.apache.kafka: WARN`. Context-rich logs include topic, partition, offset, and key for each message. Optional JSON or structured logging can be enabled via Logback appenders for integration with ELK or OpenTelemetry.
- ✅ **Global Failure Listener** — A `GlobalProducerListener` bean receives callbacks when Kafka exhausts all retries.
  - Logs failed records with topic, key, and exception details.
  - Pushes them into an in-memory queue or outbox for deferred retry.
  - Increments Prometheus metrics and may trigger circuit breakers on repeated failures.
  - Keeps `onError()` **non-blocking**, delegating heavy recovery to background re-senders.

### 🧩 How the Producer Works

The producer collects events submitted through `send()` and temporarily buffers them in Kafka’s internal **RecordAccumulator**.  
Every **5 ms** (`linger.ms=5`), the Kafka client automatically groups the accumulated events per partition into a **compressed batch** and sends it as a single network request.  
The broker then stores each event individually and waits for acknowledgments from **all in-sync replicas** (`acks=all`).  
If a batch fails to send, the client **automatically retries up to 3 times**, and if all retries fail, that batch is discarded while new events continue flowing.  
This achieves **durable, high-throughput, at-least-once delivery** without manual batching logic in the application.


---
## 📡 Consumer Module

This module demonstrates a **Spring Boot 3.x Kafka consumer** designed for **dependable event processing**, **manual/batch commits**, and **DLT handling**.

---

### ⚙️ Features

- ✅ **Manual Offset Control** —  
  `enable-auto-commit: false` and `ack-mode: batch` ensure *at-least-once* delivery by committing offsets only after successful processing.
- ✅ **Configurable Concurrency** —  
    `concurrency: 3` enables parallel (virtual-threaded) consumption, limited by the number of topic partitions._
- ✅ **Structured Retry Logic** —  
  `@RetryableTopic` provides non-blocking retries (4 total attempts) with exponential back-off before routing to a DLT.
- ✅ **Dead-Letter Routing** —  
  Unrecoverable or excluded exceptions (e.g., `NonRetryableBusinessException`) are redirected to `popov-rnd-topic-dlt`.
- ✅ **Type-Safe JSON Deserialization** — `JsonDeserializer` bound to `MyEvent` class (dev only!)
- ✅ **Payload Validation** —  
  `@Valid` triggers Jakarta Validation to stop malformed events before business logic.
- ✅ **Back-Pressure Control** —  
  `max.poll.records=500` and fetch tuning balance throughput and latency.
- ✅ **Idempotent Processing Hook** —  
  Placeholder `keyIsNotProcessed(key)` supports Redis/DB-based deduplication for safe re-delivery.
- ✅ **Virtual Thread Execution** —  
  Uses **Project Loom** (`spring.threads.virtual.enabled=true`) for lightweight concurrency and non-blocking event handling.

#### Special Note on Virtual Threads and Concurrent Message Listener Containers

Because of certain limitations in the underlying library classes still using synchronized blocks for thread coordination, applications need to be cautious when using virtual threads with concurrent message listener containers. When virtual threads are enabled, if the concurrency exceeds the available number of platform threads, it is very likely for the virtual threads to be pinned on the platform threads and possible race conditions. Therefore, as the 3rd party libraries that Spring for Apache Kafka uses evolves to fully support virtual threads, it is recommended to keep the concurrency on the message listener container to be equal to or less than the number of platform threads. This way, the applications avoid any race conditions between the threads and the virtual threads being pinned on platform threads.

See official Spring Boot doc

https://docs.spring.io/spring-kafka/reference/kafka/thread-safety.html#_special_note_on_virtual_threads_and_concurrent_message_listener_containers

### 🧩 How the Consumer Works
This consumer leverages manual offset control to guarantee at-least-once delivery, committing offsets only after each batch is successfully processed.
Incoming JSON messages are type-safely deserialized into MyEvent objects and validated before any business logic executes.
Retries are handled transparently through @RetryableTopic, which applies exponential back-off and isolates failed messages in a DLT.
Through configurable concurrency and virtual threads, it achieves efficient parallelism without blocking.
Finally, back-pressure tuning and an idempotent key check ensure stable, duplicate-free event processing under high load.

## Observability side notes

In Kafka-based systems, observability is primarily client-centric rather than broker-centric.
The Spring Boot producer and consumer expose detailed Micrometer metrics such as send rate, retries, lag, and commit latency via /actuator/prometheus.
These metrics provide the most accurate insight into message flow, latency, and processing health.
The Kafka broker itself only exposes low-level JMX metrics (replication, controller, disk I/O) and requires a JMX Exporter for Prometheus integration.
In modern setups, Prometheus and Grafana typically visualize both — application-level metrics for SLAs and broker-level metrics for cluster operations.

This demo focuses purely on producer–consumer reliability and behavior, not full production monitoring.