# Notification producer, consumer, topic/s and event/s

## Notification events
- payment-approved-notification
- payment-failed-notification

## Topic configs
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

### Consumer configs

kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.payment.lifecycle \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group digitalpayments-notification-dispatcher-v1 \
  --property max.poll.records=200 \
  --property session.timeout.ms=15000 \
  --property heartbeat.interval.ms=5000 \
  --property auto.offset.reset=earliest \
  --property enable.auto.commit=false \
  --property auto.commit.interval.ms=5000 \
  --property max.poll.interval.ms=120000 \
  --property fetch.min.bytes=1 \
  --property fetch.max.wait.ms=25 \
  --property max.partition.fetch.bytes=1048576 \
  --property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

### Producer configs

kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments.notification.status \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property acks=all \
  --property retries=10 \
  --property max.in.flight.requests.per.connection=5 \
  --property enable.idempotence=true \
  --property compression.type=lz4 \
  --property linger.ms=10 \
  --property batch.size=32768 \
  --property delivery.timeout.ms=120000 \
  --property request.timeout.ms=30000