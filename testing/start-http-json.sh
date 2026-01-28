#!/bin/sh

docker compose down -v connect
bash -c "cd .. && mvn clean package"
docker compose up -d

kafka-topics --bootstrap-server localhost:9092 --create --topic raw

# wait until http://localhost:8083 is available
while [ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083/)" != "200" ]; do
  echo "Waiting for Kafka Connect REST API to be available..."
  sleep 5
done

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d @- http://localhost:8083/connectors << EOF
{
  "name": "http",
  "config": {
    "name": "http",
    "connector.class": "io.confluent.ps.netty.NettySourceConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": false,
    "kafka.topic": "raw",
    "netty.bind.address": "0.0.0.0",
    "netty.listen.port": "8080",
    "netty.protocol": "http"
  }
}
EOF

docker logs connect -f
# kafka-console-consumer --bootstrap-server localhost:9092 --topic raw --from-beginning

# curl -vvvv -d '{"msg": "test message"}' "http://localhost:8080/api?param1=value1&param2=value2&param1=value3"