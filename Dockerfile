FROM eclipse-temurin:8-jre
ARG SERVICE_NAME=ProjectionSvr
ARG MAIN_CLASS=ProjectionSvr
ARG REPO_URL
ARG SERVICE_REVISION=unknown
ARG COMMON_REVISION=unknown
ENV DEPLOY_ROOT=/srv/dc \
    HOME=/srv/dc \
    ATS_ROOT=/srv/dc \
    BACKEND_ROOT=/srv/dc \
    SERVICE_NAME=${SERVICE_NAME} \
    MAIN_CLASS=${MAIN_CLASS} \
    TZ=Asia/Shanghai
LABEL org.opencontainers.image.source=$REPO_URL org.opencontainers.image.revision=$SERVICE_REVISION dc.common.revision=$COMMON_REVISION
WORKDIR /srv/dc/dc/${SERVICE_NAME}
COPY target/classes/ /srv/dc/dc/${SERVICE_NAME}/classes/
COPY target/dependency/ /srv/dc/dc/${SERVICE_NAME}/lib/
COPY salt-formula/ProjectionSvr/files/config/ /srv/dc/dc/${SERVICE_NAME}/config/
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh && mkdir -p /srv/dc/control /srv/dc/data /srv/dc/log
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
