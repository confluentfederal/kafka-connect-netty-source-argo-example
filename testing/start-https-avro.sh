#!/bin/sh

CERTS_DIR="../target/certs"

docker compose down -v connect
bash -c "cd .. && mvn clean package"

# Create certs
mkdir -p ${CERTS_DIR}
# Create a CA
openssl genrsa -aes256 -out ${CERTS_DIR}/ca.key -passout pass:password 4096
openssl req -x509 -new -nodes -key ${CERTS_DIR}/ca.key -passin pass:password -sha256 -days 3650 -out ${CERTS_DIR}/ca.crt -subj '/CN=Demo Root CA/C=US/ST=PA/L=Whitehall/O=Confluent'
keytool -import -v -trustcacerts -file ${CERTS_DIR}/ca.crt -keystore ${CERTS_DIR}/ca.jks -storepass password -noprompt -storetype PKCS12
# Create connect cert
openssl req -new -nodes -out ${CERTS_DIR}/connect.csr -newkey rsa:4096 -keyout ${CERTS_DIR}/connect.key -subj '/CN=connect/C=US/ST=PA/L=Whitehall/O=Confluent' -addext "subjectAltName = DNS:localhost, DNS:connect"
openssl x509 -req -in ${CERTS_DIR}/connect.csr -CA ${CERTS_DIR}/ca.crt -CAkey ${CERTS_DIR}/ca.key -passin pass:password -CAcreateserial -out ${CERTS_DIR}/connect.crt -days 3650 -sha256 -copy_extensions copyall
cat ${CERTS_DIR}/connect.key > ${CERTS_DIR}/connect-combined.crt
cat ${CERTS_DIR}/connect.crt >> ${CERTS_DIR}/connect-combined.crt
openssl pkcs12 -export -inkey ${CERTS_DIR}/connect.key -in ${CERTS_DIR}/connect.crt -passout pass:password -out ${CERTS_DIR}/connect.p12
keytool -importkeystore -deststorepass password -destkeypass password -destkeystore ${CERTS_DIR}/connect.jks -srckeystore ${CERTS_DIR}/connect.p12 -srcstoretype PKCS12 -srcstorepass password

docker compose up -d

kafka-topics --bootstrap-server localhost:9092 --create --topic raw

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d @- http://localhost:8083/connectors << EOF
{
  "name": "https",
  "config": {
    "name": "https",
    "connector.class": "io.confluent.ps.netty.NettySourceConnector",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": false,
    "kafka.topic": "raw",
    "netty.bind.address": "0.0.0.0",
    "netty.listen.port": "8443",
    "netty.protocol": "https",
    "netty.ssl.keystore.location": "/opt/kafka-connect-netty-source/certs/connect.jks",
    "netty.ssl.keystore.password": "password",
    "netty.ssl.keystore.type": "JKS",
    "netty.ssl.key.password": "password",
    "netty.ssl.truststore.location": "/opt/kafka-connect-netty-source/certs/ca.jks",
    "netty.ssl.truststore.password": "password",
    "netty.ssl.truststore.type": "JKS"
  }
}
EOF

docker logs connect -f
# kafka-console-consumer --bootstrap-server localhost:9092 --topic raw --from-beginning

# for i in `seq 1 1000`; do
#   curl -vvvv -k -X "POST" -d "{\"msg\": \"test message $i\"}" "https://localhost:8443/api?param1=value1&param2=value$i&param1=value3"
# done
