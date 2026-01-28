#!/bin/sh

docker compose down -v connect
bash -c "cd .. && mvn clean package"
docker compose up -d

kafka-topics --bootstrap-server localhost:9092 --create --topic raw

curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d @- http://localhost:8083/connectors << EOF
{
  "name": "netflow_netty_udp_9080",
  "config": {
    "name": "netflow_netty_udp_9080",
    "connector.class": "io.confluent.ps.netty.NettySourceConnector",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "kafka.topic": "raw",
    "netty.bind.address": "0.0.0.0",
    "netty.listen.port": "9080",
    "netty.protocol": "udp"
  }
}
EOF

# For some unknown reason, on MacOS netcat doesn't work well with UDP to docker containers.
# for i in $(seq 1 100000); do
#   docker exec -ti netcat sh -c "echo test $i | nc -u -w0 connect 9080"
# done

docker logs connect -f
# kafka-console-consumer --bootstrap-server localhost:9092 --topic raw --from-beginning
