#!/bin/bash

PROXY_HOME=$(cd "$(dirname "$0")/.." && pwd)
CONF_DIR="${PROXY_HOME}/conf"
LIB_DIR="${PROXY_HOME}/lib"

CLASSPATH="${CONF_DIR}"
for jar in "${LIB_DIR}"/*.jar; do
    CLASSPATH="${CLASSPATH}:${jar}"
done

JAVA_OPT="${JAVA_OPT} -server"
JAVA_OPT="${JAVA_OPT} -Xms512m -Xmx512m"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC"
JAVA_OPT="${JAVA_OPT} -XX:MaxGCPauseMillis=50"
JAVA_OPT="${JAVA_OPT} -Xlog:gc*:${PROXY_HOME}/logs/gc.log:time,uptime:filecount=5,filesize=10m"
JAVA_OPT="${JAVA_OPT} -Dlogback.configurationFile=${CONF_DIR}/logback.xml"

CONFIG_FILE="${CONF_DIR}/proxy.properties"

if [ -f "${CONFIG_FILE}" ]; then
    JAVA_OPT="${JAVA_OPT} -Dproxy.config.file=${CONFIG_FILE}"
fi

MAIN_CLASS="com.mq.proxy.core.ProxyStartup"

echo "Starting MQ Proxy..."
echo "  PROXY_HOME: ${PROXY_HOME}"
echo "  CONFIG: ${CONFIG_FILE}"
echo "  CLASSPATH: ${CLASSPATH}"

java ${JAVA_OPT} -cp "${CLASSPATH}" ${MAIN_CLASS} -c "${CONFIG_FILE}"
