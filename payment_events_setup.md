# Digital Payments Event Streaming Infrastructure

## Assumptions

- The local Kafka cluster is already deployed in Kubernetes and exposed through `kafka-service:9092`.
- Broker pods are reachable as `kafka-0`, `kafka-1`, and `kafka-2`.
- This document is a POC design and runbook. Commands are ready to run, and the output snippets below are representative examples to include after execution.
- Event payloads are JSON for readability in a CLI-driven POC. In production, Avro or Protobuf would be the next step once a schema registry is available.

## 1. Design Decisions

### 1.1 Topic Design

#### Payment lifecycle topic

- Topic name: `digitalpayments.payment.lifecycle`
- Purpose: canonical stream of immutable payment state transitions for fraud, notification, reconciliation, and audit consumers.
- Partitions: `120`
- Replication factor: `3`
- Retention: `220752000000` ms, which is 7 years.
- Cleanup policy: `delete`
- Compression: `lz4`
- Min in-sync replicas: `2`
- Max message bytes: `1048576`

Justification:

- The peak platform target is `1000 Mb/s`.
- A single consumer instance is assumed to sustain `10 Mb/s`, so the platform needs about `100` parallel consumption lanes at peak.
- `120` partitions gives the required `100` lanes plus headroom for rebalancing, uneven keys, and future traffic growth.
- Replication factor `3` matches the durability requirement and allows one broker failure without losing availability.
- `min.insync.replicas=2` with producer `acks=all` prevents acknowledged writes from being stored on only one replica.
- Payment lifecycle data is an immutable event stream, not a latest-state topic, so log compaction is the wrong cleanup model.
- Seven-year retention is mandatory for audit and fraud investigations. I intentionally rely on time-based retention only for this topic, because a size-based limit could delete compliant data early.
- `lz4` gives a better latency profile than `gzip` while still reducing bandwidth and storage costs materially.

#### Fraud score topic

- Topic name: `digitalpayments.fraud.score`
- Purpose: derived fraud decisions emitted from `payment-initiated` events.
- Partitions: `36`
- Replication factor: `3`
- Retention: `220752000000` ms, which is 7 years.
- Cleanup policy: `delete`
- Compression: `lz4`
- Min in-sync replicas: `2`
- Max message bytes: `1048576`

Justification:

- Fraud events are a subset of payment lifecycle traffic, so the topic does not need the same partition count as the primary lifecycle stream.
- `36` partitions is enough to scale fraud processors horizontally while keeping operational overhead lower than the payment topic.
- Retention stays at 7 years because fraud scores are regulated evidence and useful for replay and model back-testing.

#### Notification status topic

- Topic name: `digitalpayments.notification.status`
- Purpose: derived customer-facing notification events such as approved or failed payment status.
- Partitions: `36`
- Replication factor: `3`
- Retention: `2592000000` ms, which is 30 days.
- Cleanup policy: `delete`
- Compression: `lz4`
- Min in-sync replicas: `2`
- Max message bytes: `1048576`

Justification:

- Notifications are operational rather than regulatory records, so a 30-day replay window is sufficient for support and troubleshooting.
- The notification stream is lower risk than the payment ledger itself, but it still benefits from RF `3` and `acks=all` so customers do not miss important messages during a broker failure.

### 1.2 Message Schema

All topics use `paymentId` as the key so that every event related to the same payment lands on the same partition and remains ordered.

Common payment event fields:

| Field | Type | Required | Reason |
| --- | --- | --- | --- |
| `eventId` | string | yes | Unique event identity for deduplication and audit |
| `eventType` | string | yes | State transition or derived event name |
| `eventVersion` | integer | yes | Schema evolution support |
| `paymentId` | string | yes | Partition key and payment correlation ID |
| `correlationId` | string | yes | End-to-end request tracing |
| `traceId` | string | yes | Observability across services |
| `occurredAt` | string ISO-8601 | yes | Accurate event ordering by business time |
| `customerId` | string | yes | Downstream fraud and notification routing |
| `accountId` | string | yes | Reconciliation and audit linkage |
| `amount` | decimal | yes | Fraud scoring and ledger value |
| `currency` | string | yes | Cross-border handling and audit |
| `channel` | string | yes | Mobile app, web, ATM, API, and so on |
| `paymentMethod` | string | yes | Card, EFT, wallet, instant payment |
| `merchantId` | string | no | Merchant risk rules and reporting |
| `status` | string | yes | Business-readable state |
| `reasonCode` | string | no | Failure or rule outcome for rejected flows |
| `authCode` | string | no | Authorization trace when present |
| `metadata` | object | no | Extensible details without breaking core schema |

Fraud score fields in addition to common identifiers:

| Field | Type | Required | Reason |
| --- | --- | --- | --- |
| `fraudScore` | integer | yes | Numerical score for analytics and replay |
| `riskBand` | string | yes | `low`, `medium`, or `high` |
| `decisionReason` | string | yes | Explains why the score was produced |

Notification event fields in addition to common identifiers:

| Field | Type | Required | Reason |
| --- | --- | --- | --- |
| `notificationType` | string | yes | Approved or failed notification |
| `destination` | string | yes | Email, SMS, push, or in-app |
| `templateId` | string | yes | Links event to message template |

### 1.3 Producer Design

#### Payment producer

- Published events:
	- `payment-initiated`
	- `payment-authorised`
	- `payment-not-authorised`
	- `payment-validated`
	- `payment-invalidated`
	- `payment-completed`
	- `payment-not-completed`
- Partition key: `paymentId`
- Durability: `acks=all`
- Idempotency: `enable.idempotence=true`
- Serialization: JSON
- Compression: `lz4`
- Retry strategy: `retries=10`, `request.timeout.ms=30000`, `delivery.timeout.ms=120000`
- In-flight requests: `max.in.flight.requests.per.connection=5`
- Batching: `linger.ms=5`, `batch.size=65536`

Justification:

- `paymentId` preserves the full lifecycle ordering for each payment, which is the main downstream correctness requirement.
- `acks=all` is mandatory because losing a payment initiation or completion event is materially worse than a small latency increase.
- Idempotence prevents duplicate records when the producer retries after a transient network or leader issue.
- JSON is larger than Avro or Protobuf, but it is the right POC choice because it works directly with Kafka CLI tools and is easy to review in markdown evidence.
- A two-minute delivery timeout is a practical upper bound for a payment event to be buffered in a POC. Beyond that, the producer should fail and surface an operational alert.

#### Fraud producer

- Input: `payment-initiated` events from the lifecycle topic.
- Output events:
	- `fraud-score-low`
	- `fraud-score-medium`
	- `fraud-score-high`
- Partition key: `paymentId`
- Durability: `acks=all`
- Idempotency: `enable.idempotence=true`
- Low-latency tuning: `linger.ms=1`, `batch.size=32768`, `request.timeout.ms=15000`, `delivery.timeout.ms=60000`

Justification:

- Fraud detection has the tightest SLA in the system, below `50 ms`, so the producer uses smaller batches and shorter linger time than the payment lifecycle producer.

#### Notification producer

- Input: payment validation or failure outcomes from the lifecycle topic.
- Output events:
	- `payment-approved-notification`
	- `payment-failed-notification`
- Partition key: `paymentId`
- Durability: `acks=all`
- Idempotency: `enable.idempotence=true`
- Tuning: `linger.ms=10`, `batch.size=32768`, `request.timeout.ms=30000`, `delivery.timeout.ms=120000`

Justification:

- Notifications can tolerate slightly more batching than fraud scoring, but not enough delay to threaten the `< 2s` customer SLA.

### 1.4 Consumer Design

I designed three independent consumer groups, which satisfies the minimum project requirement and matches the three clear downstream concerns.

#### Consumer group 1: fraud scoring

- Consumer group name: `digitalpayments-fraud-detector-v1`
- Reads: `digitalpayments.payment.lifecycle`
- Filters: `payment-initiated`
- SLA: `< 50 ms` processing lag target
- Offset strategy: manual commit after the fraud score is published successfully
- Scalability: horizontally scalable up to `120` instances if needed, because it consumes from the lifecycle topic

Processing logic:

1. Read a `payment-initiated` event.
2. Apply scoring rules based on amount, channel, payment method, merchant, and customer history.
3. Publish a fraud score event to `digitalpayments.fraud.score`.
4. Commit the offset only after the score event is produced successfully.

#### Consumer group 2: notification dispatcher

- Consumer group name: `digitalpayments-notification-dispatcher-v1`
- Reads: `digitalpayments.payment.lifecycle`
- Filters: `payment-validated`, `payment-not-authorised`, `payment-invalidated`, `payment-not-completed`
- SLA: `< 2 s` processing lag target
- Offset strategy: manual commit after the notification event is published successfully
- Scalability: horizontally scalable up to `120` instances

Processing logic:

1. Read lifecycle events.
2. Transform successful validation into `payment-approved-notification`.
3. Transform failure outcomes into `payment-failed-notification`.
4. Publish to `digitalpayments.notification.status`.
5. Commit the offset only after the derived event is produced.

#### Consumer group 3: payment audit and reconciliation

- Consumer group name: `digitalpayments-payment-audit-v1`
- Reads: `digitalpayments.payment.lifecycle`
- Filters: none, consumes the full stream
- SLA: near real-time preferred, but can tolerate lag in minutes during recovery
- Offset strategy: manual commit after audit persistence succeeds
- Scalability: horizontally scalable up to `120` instances, though practical deployment would usually use fewer workers because it processes all events

Processing logic:

1. Consume every lifecycle event.
2. Persist the event to the audit store or reconciliation sink.
3. Validate that each payment journey has a legal state transition sequence.
4. Raise an operational alert if transitions are missing or duplicated.
5. Commit offsets only after durable persistence succeeds.

## 2. Topic Creation

### 2.1 Cluster verification and startup checks

Because the local cluster is provided, the first operational step is to verify that the Kubernetes resources are healthy rather than provisioning Kafka from scratch.

```bash
kubectl get pods
kubectl get svc kafka-service
kubectl rollout status statefulset/kafka
kubectl exec -it kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --list
```

Expected outcome:

- `kafka-0`, `kafka-1`, and `kafka-2` are running.
- `kafka-service` resolves on port `9092`.
- The broker responds to topic metadata requests.

### 2.2 Topic creation commands

```bash
kubectl exec -it kafka-0 -- kafka-topics \
	--bootstrap-server kafka-service:9092 \
	--create \
	--topic digitalpayments.payment.lifecycle \
	--partitions 120 \
	--replication-factor 3 \
	--config retention.ms=220752000000 \
	--config cleanup.policy=delete \
	--config min.insync.replicas=2 \
	--config compression.type=lz4 \
	--config max.message.bytes=1048576
```

```bash
kubectl exec -it kafka-0 -- kafka-topics \
	--bootstrap-server kafka-service:9092 \
	--create \
	--topic digitalpayments.fraud.score \
	--partitions 36 \
	--replication-factor 3 \
	--config retention.ms=220752000000 \
	--config cleanup.policy=delete \
	--config min.insync.replicas=2 \
	--config compression.type=lz4 \
	--config max.message.bytes=1048576
```

```bash
kubectl exec -it kafka-0 -- kafka-topics \
	--bootstrap-server kafka-service:9092 \
	--create \
	--topic digitalpayments.notification.status \
	--partitions 36 \
	--replication-factor 3 \
	--config retention.ms=2592000000 \
	--config cleanup.policy=delete \
	--config min.insync.replicas=2 \
	--config compression.type=lz4 \
	--config max.message.bytes=1048576
```

### 2.3 Topic verification commands

```bash
kubectl exec -it kafka-0 -- kafka-topics \
	--bootstrap-server kafka-service:9092 \
	--describe --topic digitalpayments.payment.lifecycle
```

```bash
kubectl exec -it kafka-0 -- kafka-topics \
	--bootstrap-server kafka-service:9092 \
	--describe --topic digitalpayments.fraud.score
```

```bash
kubectl exec -it kafka-0 -- kafka-topics \
	--bootstrap-server kafka-service:9092 \
	--describe --topic digitalpayments.notification.status
```

Representative output:

```text
Topic: digitalpayments.payment.lifecycle    PartitionCount: 120    ReplicationFactor: 3    Configs: cleanup.policy=delete,compression.type=lz4,max.message.bytes=1048576,min.insync.replicas=2,retention.ms=220752000000
		Topic: digitalpayments.payment.lifecycle    Partition: 0      Leader: 0    Replicas: 0,1,2    Isr: 0,1,2
		Topic: digitalpayments.payment.lifecycle    Partition: 1      Leader: 1    Replicas: 1,2,0    Isr: 1,2,0

Topic: digitalpayments.fraud.score    PartitionCount: 36    ReplicationFactor: 3    Configs: cleanup.policy=delete,compression.type=lz4,max.message.bytes=1048576,min.insync.replicas=2,retention.ms=220752000000
		Topic: digitalpayments.fraud.score    Partition: 0      Leader: 0    Replicas: 0,1,2    Isr: 0,1,2

Topic: digitalpayments.notification.status    PartitionCount: 36    ReplicationFactor: 3    Configs: cleanup.policy=delete,compression.type=lz4,max.message.bytes=1048576,min.insync.replicas=2,retention.ms=2592000000
		Topic: digitalpayments.notification.status    Partition: 0      Leader: 1    Replicas: 1,2,0    Isr: 1,2,0
```

## 3. Producer Setup

### 3.1 Producer choice

For the POC I would use `kafka-console-producer` to generate the payment lifecycle stream because the brief explicitly allows CLI tools and the console producer keeps the evidence readable. For the fraud and notification processors, a thin script is the better choice because those consumers must filter events, perform logic, and only then publish derived topics and commit offsets.

### 3.2 Payment producer command

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
	--bootstrap-server kafka-service:9092 \
	--topic digitalpayments.payment.lifecycle \
	--property parse.key=true \
	--property key.separator=: \
	--property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
	--property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
	--property acks=all \
	--property retries=10 \
	--property max.in.flight.requests.per.connection=5 \
	--property enable.idempotence=true \
	--property compression.type=lz4 \
	--property linger.ms=5 \
	--property batch.size=65536 \
	--property delivery.timeout.ms=120000 \
	--property request.timeout.ms=30000
```

### 3.3 Mock event volume

I would generate `100` payment journeys to cover success and failure paths with enough volume to test ordering, lag, and derived topics.

Suggested mix:

- `70` successful journeys: initiated -> authorised -> validated -> completed
- `15` rejected journeys: initiated -> not-authorised
- `10` invalidated journeys: initiated -> authorised -> invalidated
- `5` completion failures: initiated -> authorised -> validated -> not-completed

This produces `360` lifecycle events in total:

- `280 + 30 + 30 + 20 = 360`

### 3.4 Example lifecycle events

Successful payment journey:

```text
pay-1000001:{"eventId":"evt-1000001-01","eventType":"payment-initiated","eventVersion":1,"paymentId":"pay-1000001","correlationId":"corr-1000001","traceId":"trace-1000001","occurredAt":"2026-09-28T09:00:00Z","customerId":"cust-001","accountId":"acct-901","amount":1250.75,"currency":"ZAR","channel":"mobile-app","paymentMethod":"instant-payment","merchantId":"mrc-100","status":"initiated","metadata":{"deviceId":"dev-01"}}
pay-1000001:{"eventId":"evt-1000001-02","eventType":"payment-authorised","eventVersion":1,"paymentId":"pay-1000001","correlationId":"corr-1000001","traceId":"trace-1000001","occurredAt":"2026-09-28T09:00:01Z","customerId":"cust-001","accountId":"acct-901","amount":1250.75,"currency":"ZAR","channel":"mobile-app","paymentMethod":"instant-payment","merchantId":"mrc-100","status":"authorised","authCode":"AUTH-884211"}
pay-1000001:{"eventId":"evt-1000001-03","eventType":"payment-validated","eventVersion":1,"paymentId":"pay-1000001","correlationId":"corr-1000001","traceId":"trace-1000001","occurredAt":"2026-09-28T09:00:01.050Z","customerId":"cust-001","accountId":"acct-901","amount":1250.75,"currency":"ZAR","channel":"mobile-app","paymentMethod":"instant-payment","merchantId":"mrc-100","status":"validated"}
pay-1000001:{"eventId":"evt-1000001-04","eventType":"payment-completed","eventVersion":1,"paymentId":"pay-1000001","correlationId":"corr-1000001","traceId":"trace-1000001","occurredAt":"2026-09-28T09:00:01.120Z","customerId":"cust-001","accountId":"acct-901","amount":1250.75,"currency":"ZAR","channel":"mobile-app","paymentMethod":"instant-payment","merchantId":"mrc-100","status":"completed"}
```

Failed payment journey:

```text
pay-1000031:{"eventId":"evt-1000031-01","eventType":"payment-initiated","eventVersion":1,"paymentId":"pay-1000031","correlationId":"corr-1000031","traceId":"trace-1000031","occurredAt":"2026-09-28T09:03:00Z","customerId":"cust-031","accountId":"acct-931","amount":9800.00,"currency":"ZAR","channel":"web","paymentMethod":"card","merchantId":"mrc-700","status":"initiated"}
pay-1000031:{"eventId":"evt-1000031-02","eventType":"payment-not-authorised","eventVersion":1,"paymentId":"pay-1000031","correlationId":"corr-1000031","traceId":"trace-1000031","occurredAt":"2026-09-28T09:03:00.040Z","customerId":"cust-031","accountId":"acct-931","amount":9800.00,"currency":"ZAR","channel":"web","paymentMethod":"card","merchantId":"mrc-700","status":"failed","reasonCode":"AUTH_DECLINED"}
```

### 3.5 Evidence of successful production

Use a read-only verification consumer to inspect key, partition, and offset values:

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
	--bootstrap-server kafka-service:9092 \
	--topic digitalpayments.payment.lifecycle \
	--from-beginning \
	--max-messages 8 \
	--property print.key=true \
	--property print.partition=true \
	--property print.offset=true
```

Representative output proving ordering for a single payment key:

```text
Partition:41    Offset:0    Key:pay-1000001    {"eventId":"evt-1000001-01","eventType":"payment-initiated",...}
Partition:41    Offset:1    Key:pay-1000001    {"eventId":"evt-1000001-02","eventType":"payment-authorised",...}
Partition:41    Offset:2    Key:pay-1000001    {"eventId":"evt-1000001-03","eventType":"payment-validated",...}
Partition:41    Offset:3    Key:pay-1000001    {"eventId":"evt-1000001-04","eventType":"payment-completed",...}
Partition:88    Offset:0    Key:pay-1000031    {"eventId":"evt-1000031-01","eventType":"payment-initiated",...}
Partition:88    Offset:1    Key:pay-1000031    {"eventId":"evt-1000031-02","eventType":"payment-not-authorised",...}
```

Interpretation:

- All events for `pay-1000001` stayed on partition `41` and maintained increasing offsets.
- All events for `pay-1000031` stayed on partition `88` and maintained increasing offsets.
- Different payment IDs can land on different partitions, which is desirable for throughput.

## 4. Consumer Groups

### 4.1 Fraud detector consumer

Inspection command:

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
	--bootstrap-server kafka-service:9092 \
	--topic digitalpayments.payment.lifecycle \
	--group digitalpayments-fraud-detector-v1 \
	--property parse.key=true \
	--property print.key=true \
	--property print.partition=true \
	--property print.offset=true \
	--property max.poll.records=100 \
	--property session.timeout.ms=9000 \
	--property heartbeat.interval.ms=3000 \
	--property auto.offset.reset=earliest \
	--property enable.auto.commit=false \
	--property max.poll.interval.ms=60000 \
	--property fetch.min.bytes=1 \
	--property fetch.max.wait.ms=10 \
	--property max.partition.fetch.bytes=524288
```

POC processing rule:

- Only process events where `eventType=payment-initiated`.
- Publish `fraud-score-low`, `fraud-score-medium`, or `fraud-score-high` to `digitalpayments.fraud.score`.

Representative downstream output:

```text
pay-1000001 {"eventId":"frd-1000001-01","eventType":"fraud-score-low","paymentId":"pay-1000001","fraudScore":18,"riskBand":"low","decisionReason":"known-device low-value payment"}
pay-1000031 {"eventId":"frd-1000031-01","eventType":"fraud-score-high","paymentId":"pay-1000031","fraudScore":91,"riskBand":"high","decisionReason":"high-value card payment from new channel"}
```

### 4.2 Notification dispatcher consumer

Inspection command:

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
	--bootstrap-server kafka-service:9092 \
	--topic digitalpayments.payment.lifecycle \
	--group digitalpayments-notification-dispatcher-v1 \
	--property parse.key=true \
	--property print.key=true \
	--property print.partition=true \
	--property print.offset=true \
	--property max.poll.records=200 \
	--property session.timeout.ms=15000 \
	--property heartbeat.interval.ms=5000 \
	--property auto.offset.reset=earliest \
	--property enable.auto.commit=false \
	--property max.poll.interval.ms=120000 \
	--property fetch.min.bytes=1 \
	--property fetch.max.wait.ms=25 \
	--property max.partition.fetch.bytes=1048576
```

POC processing rule:

- Transform `payment-validated` into `payment-approved-notification`.
- Transform `payment-not-authorised`, `payment-invalidated`, and `payment-not-completed` into `payment-failed-notification`.

Representative downstream output:

```text
pay-1000001 {"eventId":"ntf-1000001-01","eventType":"payment-approved-notification","paymentId":"pay-1000001","notificationType":"approved","destination":"push","templateId":"payment-approved-v1"}
pay-1000031 {"eventId":"ntf-1000031-01","eventType":"payment-failed-notification","paymentId":"pay-1000031","notificationType":"failed","destination":"sms","templateId":"payment-failed-v1"}
```

### 4.3 Payment audit consumer

Inspection command:

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
	--bootstrap-server kafka-service:9092 \
	--topic digitalpayments.payment.lifecycle \
	--group digitalpayments-payment-audit-v1 \
	--property parse.key=true \
	--property print.key=true \
	--property print.partition=true \
	--property print.offset=true \
	--property max.poll.records=500 \
	--property session.timeout.ms=15000 \
	--property heartbeat.interval.ms=5000 \
	--property auto.offset.reset=earliest \
	--property enable.auto.commit=false \
	--property max.poll.interval.ms=300000 \
	--property fetch.min.bytes=1 \
	--property fetch.max.wait.ms=50 \
	--property max.partition.fetch.bytes=1048576
```

Processing rule:

- Persist every lifecycle event exactly as received.
- Rebuild payment journeys for reconciliation and compliance review.

Representative output:

```text
pay-1000001 {"eventId":"evt-1000001-01","eventType":"payment-initiated",...}
pay-1000001 {"eventId":"evt-1000001-02","eventType":"payment-authorised",...}
pay-1000001 {"eventId":"evt-1000001-03","eventType":"payment-validated",...}
pay-1000001 {"eventId":"evt-1000001-04","eventType":"payment-completed",...}
```

## 5. Verification and Monitoring

### 5.1 Consumer group status commands

```bash
kubectl exec -it kafka-0 -- kafka-consumer-groups \
	--bootstrap-server kafka-service:9092 \
	--describe --group digitalpayments-fraud-detector-v1
```

```bash
kubectl exec -it kafka-0 -- kafka-consumer-groups \
	--bootstrap-server kafka-service:9092 \
	--describe --group digitalpayments-notification-dispatcher-v1
```

```bash
kubectl exec -it kafka-0 -- kafka-consumer-groups \
	--bootstrap-server kafka-service:9092 \
	--describe --group digitalpayments-payment-audit-v1
```

Representative lag output:

```text
GROUP                                      TOPIC                              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
digitalpayments-fraud-detector-v1         digitalpayments.payment.lifecycle  41         4               4               0    consumer-1
digitalpayments-notification-dispatcher-v1 digitalpayments.payment.lifecycle  41         4               4               0    consumer-2
digitalpayments-payment-audit-v1          digitalpayments.payment.lifecycle  41         4               4               0    consumer-3
```

Interpretation:

- Lag at `0` means all three groups are caught up.
- If fraud lags while notification stays current, the architecture still isolates the notification path from fraud backpressure.

### 5.2 Ordering verification

Ordering is considered correct when all events for a single `paymentId` share one partition and offsets strictly increase. The lifecycle verification output above demonstrates that behavior.

### 5.3 Topic status checks

```bash
kubectl exec -it kafka-0 -- kafka-topics \
	--bootstrap-server kafka-service:9092 \
	--list
```

Expected topics:

```text
digitalpayments.payment.lifecycle
digitalpayments.fraud.score
digitalpayments.notification.status
```

## 6. Trade-offs and Justifications

### JSON vs Avro or Protobuf

- I chose JSON because this is a CLI-led POC and the events need to be readable in markdown evidence.
- The trade-off is larger payloads, weaker schema enforcement, and higher parsing cost.
- For production, I would move the same event model to Avro or Protobuf with schema compatibility rules.

### 120 payment partitions vs a smaller number

- `120` partitions is intentionally sized for the stated peak throughput and consumer parallelism assumptions rather than for a tiny local demo.
- The trade-off is higher metadata and operational overhead on a local cluster.
- I kept the sizing true to the brief because the exercise explicitly asks for design decisions justified against the stated throughput and SLA targets.

### RF 3 and min ISR 2

- This is the minimum responsible durability setting for payment events on a three-broker cluster.
- The trade-off is that writes can fail when only one replica is available, but that is the correct failure mode for payment integrity.

### Delete cleanup policy instead of compaction

- Lifecycle and fraud events are historical facts, not mutable account state.
- Compaction would keep only the latest record per key, which would destroy the audit trail.

### Manual offsets instead of auto-commit

- Fraud, notification, and audit consumers all perform work that must not be acknowledged before success.
- The trade-off is more application logic, but it avoids silently losing work after an early offset commit.

## 7. Issues Encountered

No live cluster execution was performed from this workspace, so I did not capture actual runtime failures or real broker output here. The most likely issues during execution and their fixes are:

- Topic creation fails because the broker is unavailable:
	- Verify `kafka-service:9092` is reachable and the broker pods are running.
- Consumers show no messages:
	- Confirm that the producer sent events and that the consumers used `--from-beginning` or the correct group offsets.
- Fraud or notification lag grows:
	- Increase consumer instances, confirm filter logic is not blocking, and inspect broker health.
- Events appear out of order:
	- Confirm every producer uses `paymentId` consistently as the Kafka key.
- Duplicate derived events appear:
	- Verify producer idempotence is enabled and offsets are committed only after downstream publication succeeds.

## 8. Summary

This design uses one canonical payment lifecycle topic plus two derived topics for fraud and notifications. The design optimizes for payment integrity first, then low-latency downstream processing, while preserving ordered per-payment histories for audit and replay.
