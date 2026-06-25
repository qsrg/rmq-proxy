#!/bin/bash

PROXY_HOME=$(cd "$(dirname "$0")/.." && pwd)
CONF_DIR="${PROXY_HOME}/conf"
LOG_DIR="${PROXY_HOME}/logs"

if [ -n "${JAVA_HOME}" ]; then
    JAVA_CMD="${JAVA_HOME}/bin/java"
else
    JAVA_CMD="java"
fi

USER_JAVA_OPT="${JAVA_OPT}"

JAVA_OPT="-server"
JAVA_OPT="${JAVA_OPT} -Xms1g -Xmx1g"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC"
JAVA_OPT="${JAVA_OPT} -XX:MaxGCPauseMillis=100"
JAVA_OPT="${JAVA_OPT} -XX:InitiatingHeapOccupancyPercent=45"
JAVA_OPT="${JAVA_OPT} -XX:+ParallelRefProcEnabled"
JAVA_OPT="${JAVA_OPT} -XX:+DisableExplicitGC"
JAVA_OPT="${JAVA_OPT} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR}"
JAVA_OPT="${JAVA_OPT} -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps"
JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=50M"
JAVA_OPT="${JAVA_OPT} -Xloggc:${LOG_DIR}/gc.log"
JAVA_OPT="${JAVA_OPT} -DPROXY_HOME=${PROXY_HOME}"
JAVA_OPT="${JAVA_OPT} -Dlogback.configurationFile=${CONF_DIR}/logback.xml"
JAVA_OPT="${JAVA_OPT} ${USER_JAVA_OPT}"

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
echo "  JAVA: ${JAVA_CMD}"

mkdir -p "${LOG_DIR}"

if [ -f "${CONFIG_FILE}" ]; then
    "${JAVA_CMD}" ${JAVA_OPT} -jar "${JAR_FILE}" -c "${CONFIG_FILE}"
else
    "${JAVA_CMD}" ${JAVA_OPT} -jar "${JAR_FILE}"
fi
