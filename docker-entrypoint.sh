#!/usr/bin/env bash
set -euo pipefail
DEPLOY_ROOT="${DEPLOY_ROOT:-/srv/dc}"
SERVICE_NAME="${SERVICE_NAME:-ProjectionSvr}"
MAIN_CLASS="${MAIN_CLASS:-ProjectionSvr}"
SERVICE_DIR="${DEPLOY_ROOT}/dc/${SERVICE_NAME}"
cd "${SERVICE_DIR}"
mkdir -p "${DEPLOY_ROOT}/data" "${DEPLOY_ROOT}/log"
JAVA_OPTS="${JAVA_OPTS:--server -Xms32m -Xmx192m -Xmn48m -Djava.security.auth.login.config=../../control/jaas.ini}"
exec java ${JAVA_OPTS} ${EXTRA_JAVA_OPTS:-} -Duser.home="${DEPLOY_ROOT}" -Duser.dir="${SERVICE_DIR}" \
  -cp "classes:config:lib/*" "${MAIN_CLASS}" ${APP_ARGS:-}
