#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${MODULE_DIR}/.." && pwd)"

if [[ ! -d "${MODULE_DIR}/target/classes" || ! -d "${MODULE_DIR}/target/lib" ]]; then
  (cd "${ROOT_DIR}" && mvn -pl mq-proxy-example -am package -DskipTests)
fi

exec java -cp "${MODULE_DIR}/target/classes:${MODULE_DIR}/target/lib/*" \
  com.mq.proxy.example.benchmark.RocketMQProxyBenchmark "$@"
