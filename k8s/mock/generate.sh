#!/usr/bin/env bash
# Publishes EpexMarketData JSON (PascalCase property names) for delivery areas DE and FR.
set -euo pipefail

bootstrap="${KAFKA_BOOTSTRAP:-kafka:9092}"
topic="${KAFKA_TOPIC:-epex-market-data}"
interval="${INTERVAL_SECONDS:-1}"

until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$bootstrap" >/dev/null 2>&1; do
  echo "waiting for kafka at ${bootstrap}"
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" \
  --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1

produce() {
  local markets=("Day-Ahead" "Intraday")
  local i=0
  while true; do
    local market="${markets[$((i % 2))]}"
    local ts
    ts=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)
    local area
    for area in DE FR; do
      local cents=$((1000 + RANDOM % 8000))
      local price
      price=$(printf '%d.%02d' $((cents / 100)) $((cents % 100)))
      printf '{"Symbol":"%s","Market":"%s","DeliveryArea":"%s","Timestamp":"%s","Price":%s,"Unit":"EUR/MWh","Contract":"P60"}\n' \
        "$area" "$market" "$area" "$ts" "$price"
    done
    i=$((i + 1))
    sleep "$interval"
  done
}

# --timeout is linger.ms (default 1000). The record timestamp is taken before lingering, so the
# default would add up to a second to PriceStream's publish latency.
produce | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$bootstrap" --topic "$topic" --timeout 5
