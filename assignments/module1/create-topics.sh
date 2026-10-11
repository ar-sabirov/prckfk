/usr/bin/kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}" \
  --create \
  --if-not-exists \
  --topic orders.created.protobuf \
  --partitions 3 \
  --replication-factor 2
