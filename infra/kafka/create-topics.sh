#!/bin/sh
# Invoke from the repository root; Make supplies the configured topic.
set -eu
topic=${1:?Pass KAFKA_SOCIAL_POST_TOPIC from collector configuration}
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --describe --topic "$topic"
