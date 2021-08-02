# Bifrost

### kafka
docker exec -it bifrost-kafka /bin/bash
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic publish


### TODO
1. client map -> sorted map
2. retain 中心化
3. 限制publish packet payload的长度
4. 限制Topic 和 Topic filter的长度和层级