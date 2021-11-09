### Start do Zookeper
rm -r ~/dev/server/kafka/data/* && ~/dev/server/kafka/kafka_2.12-3.0.0/bin/zookeeper-server-start.sh ~/dev/server/kafka/kafka_2.12-3.0.0/config/zookeeper.properties

### Start dos brokers (3 broker, numero configurado de 3 replicações)
~/dev/server/kafka/kafka_2.12-3.0.0/bin/kafka-server-start.sh ~/dev/server/kafka/kafka_2.12-3.0.0/config/server1.properties
~/dev/server/kafka/kafka_2.12-3.0.0/bin/kafka-server-start.sh ~/dev/server/kafka/kafka_2.12-3.0.0/config/server2.properties
~/dev/server/kafka/kafka_2.12-3.0.0/bin/kafka-server-start.sh ~/dev/server/kafka/kafka_2.12-3.0.0/config/server3.properties

cd /home/cicero/dev/server/kafka_2.12-3.0.0


bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

// CRIAR UM NOVO TIPIC
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic LOJA_NOVOPEDIDO

# listar topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Mandando mensagens para um topic
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic LOJA_NOVOPEDIDO

# consumindo topic desde o inicio
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic LOJA_NOVOPEDIDO --from-beginning

#descrever os topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

#altera o numero de partitions de um topic
./bin/kafka-topics.sh --alter --bootstrap-server localhost:9092 --topic ECOMMERCE_NEW_ORDER --partitions 3

#mostrar o status do consumo dos grupos
bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --all-groups --describe

# Simulação para novas Ordens
http://localhost:8080/new?email=cicero2@ciceroinfo.com&amount=1230&uuid=3

# Simulação para geração de reports
http://localhost:8080/admin/generate-reports