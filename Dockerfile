FROM openjdk:8-jdk-alpine
MAINTAINER <moluguravindranath.aditya@optum.com>
COPY /build/libs/*.jar /tmp/
COPY /src/main/resources/*.jks /tmp/

#Running the application
#ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/tmp/edp_trans_ingestion_log-1.0.0.jar"]

#COPY /src/main/resources/cert/kaas-truststore.jks /tmp/
#COPY /src/main/resources/cert/cdb-alpha-edp.keystore.jks /tmp/

COPY /src/main/resources/prodcert/kaas-truststore.jks /tmp/prodcert/

COPY /src/main/resources/prodcert/edp-att-prod.keystore.jks /tmp/prodcert/

COPY /src/main/resources/hcpprodcert/keystore.jks /tmp/hcpprodcert/
COPY /src/main/resources/hcpprodcert/truststore.jks /tmp/hcpprodcert/

COPY /src/main/resources/new_certs_nonprod/keystore.jks /tmp/
COPY /src/main/resources/new_certs_nonprod/truststore.jks /tmp/


USER 1000:1000

#ENTRYPOINT /bin/sh -c "java -Djava.security.egd=file:/dev/./urandom $JAVA_OPTS -Dspring.profiles.active=$ENV_PROFILE -Dspring.kafka.streams.applicationId=edp-att-prod-$APP_NAME -jar /tmp/edp_trans_ingestion_log-0.0.1-Rel.jar"

ENTRYPOINT /bin/sh -c "java -Djava.security.egd=file:/dev/./urandom $JAVA_OPTS -Dspring.profiles.active=$ENV_PROFILE -Dspring.kafka.streams.applicationId=cdb-claas-prod-edp-ingestionlog-$APP_NAME -jar /tmp/edp_trans_ingestion_log-0.0.1-Rel.jar"
#ENTRYPOINT /bin/sh -c "java -Djava.security.egd=file:/dev/./urandom $JAVA_OPTS -Dspring.profiles.active=$ENV_PROFILE -Dspring.kafka.streams.applicationId=cdb-claas-alpha-edp-ingestionlog -jar /tmp/edp_trans_ingestion_log-0.0.1-Rel.jar"

