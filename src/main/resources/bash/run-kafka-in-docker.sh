sudo sysctl -w vm.max_map_count=262144
git clone https://github.com/confluentinc/examples.git
cd examples/clickstream
git checkout 6.0.0-post
docker-compose up -d
docker-compose ps;
docker-compose rm -s;
docker exec kafka /bin/bash

bin/kafka-topics --bootstrap-server kafka:29092 --delete --topic weather_kafka
bin/kafka-topics --bootstrap-server kafka:29092 --delete --topic hotel_kafka