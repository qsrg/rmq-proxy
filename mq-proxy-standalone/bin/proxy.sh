#!/bin/bash

PROXY_HOME=$(cd "$(dirname "$0")/.." && pwd)
CONF_DIR="${PROXY_HOME}/conf"

JAVA_OPT="${JAVA_OPT} -server"
JAVA_OPT="${JAVA_OPT} -Xms512m -Xmx512m"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC"
JAVA_OPT="${JAVA_OPT} -XX:MaxGCPauseMillis=50"
JAVA_OPT="${JAVA_OPT} -Xlog:gc*:${PROXY_HOME}/logs/gc.log:time,uptime:filecount=5,filesize=10m"
JAVA_OPT="${JAVA_OPT} -Dlogback.configurationFile=${CONF_DIR}/logback.xml"

CONFIG_FILE="${CONF_DIR}/proxy.properties"

JAR_FILE=$(ls "${PROXY_HOME}"/mq-proxy-*.jar 2>/dev/null | head -1)
if [ -z "${JAR_FILE}" ]; then
    JAR_FILE=$(ls "${PROXY_HOME}"/lib/mq-proxy-standalone-*.jar 2>/dev/null | head -1)
fi

if [ -z "${JAR_FILE}" ]; then
    echo "Error: mq-proxy jar not found"
    exit 1
fi

echo "Starting MQ Proxy..."
echo "  PROXY_HOME: ${PROXY_HOME}"
echo "  CONFIG: ${CONFIG_FILE}"
echo "  JAR: ${JAR_FILE}"

if [ -f "${CONFIG_FILE}" ]; then
    java ${JAVA_OPT} -jar "${JAR_FILE}" -c "${CONFIG_FILE}"
else
    java ${JAVA_OPT} -jar "${JAR_FILE}"
fi
