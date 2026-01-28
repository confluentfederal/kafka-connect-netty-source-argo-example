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
openssl req -new -nodes -out ${CERTS_DIR}/connect.csr -newkey rsa:4096 -keyout ${CERTS_DIR}/connect.key -subj '/CN=connect.zt.disa.mil/C=US/ST=PA/L=Whitehall/O=Confluent' -addext "subjectAltName = DNS:connect0.zt.disa.mil, DNS:connect1.zt.disa.mil"
openssl x509 -req -in ${CERTS_DIR}/connect.csr -CA ${CERTS_DIR}/ca.crt -CAkey ${CERTS_DIR}/ca.key -passin pass:password -CAcreateserial -out ${CERTS_DIR}/connect.crt -days 3650 -sha256 -copy_extensions copyall
cat ${CERTS_DIR}/connect.key > ${CERTS_DIR}/connect-combined.crt
cat ${CERTS_DIR}/connect.crt >> ${CERTS_DIR}/connect-combined.crt
openssl pkcs12 -export -inkey ${CERTS_DIR}/connect.key -in ${CERTS_DIR}/connect.crt -passout pass:password -out ${CERTS_DIR}/connect.p12
keytool -importkeystore -deststorepass password -destkeypass password -destkeystore ${CERTS_DIR}/connect.jks -deststoretype PKCS12 -srckeystore ${CERTS_DIR}/connect.p12 -srcstoretype PKCS12 -srcstorepass password

# Create random cert
openssl req -new -nodes -out ${CERTS_DIR}/random.csr -newkey rsa:4096 -keyout ${CERTS_DIR}/random.key -subj '/CN=random/C=US/ST=PA/L=Whitehall/O=Confluent' -addext "subjectAltName = DNS:localhost, DNS:connect"
openssl x509 -req -in ${CERTS_DIR}/random.csr -CA ${CERTS_DIR}/ca.crt -CAkey ${CERTS_DIR}/ca.key -passin pass:password -CAcreateserial -out ${CERTS_DIR}/random.crt -days 3650 -sha256 -copy_extensions copyall
# keytool -importkeystore -deststorepass password -destkeypass password -destkeystore ${CERTS_DIR}/random.jks -deststoretype PKCS12 -srckeystore ${CERTS_DIR}/random.p12 -srcstoretype PKCS12 -srcstorepass password

# Create a super keystore
keytool -importkeystore -deststorepass password -destkeystore ${CERTS_DIR}/kitchensink.jks -deststoretype PKCS12 -destalias ca -srckeystore ${CERTS_DIR}/ca.jks -srcstoretype PKCS12 -srcstorepass password -srcalias mykey
keytool -importkeystore -deststorepass password -destkeypass password -destkeystore ${CERTS_DIR}/kitchensink.jks -deststoretype PKCS12 -destalias confluent -srckeystore ${CERTS_DIR}/connect.jks -srcstoretype PKCS12 -srcstorepass password -srcalias 1
keytool -importcert -storepass password -keystore ${CERTS_DIR}/kitchensink.jks -storetype PKCS12 -alias random -file ${CERTS_DIR}/random.crt

docker compose up -d

kafka-topics --bootstrap-server localhost:9092 --create --topic raw

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d @- http://localhost:8083/connectors << EOF
{
  "name": "tcpssl",
  "config": {
    "name": "tcpssl",
    "connector.class": "io.confluent.ps.netty.NettySourceConnector",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "kafka.topic": "raw",
    "netty.bind.address": "0.0.0.0",
    "netty.listen.port": "8443",
    "netty.protocol": "tcpssl",
    "netty.ssl.keystore.location": "/opt/kafka-connect-netty-source/certs/kitchensink.jks",
    "netty.ssl.keystore.password": "password",
    "netty.ssl.keystore.type": "PKCS12",
    "netty.ssl.key.password": "password"
  }
}
EOF

docker logs connect -f
# kafka-console-consumer --bootstrap-server localhost:9092 --topic raw --from-beginning

# openssl s_client -connect localhost:8443

# for i in `seq 1 1000`; do
#   echo "test message $i" | openssl s_client -connect localhost:8443
# done
