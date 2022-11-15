# apache-air

```docker compose up```

Nifi stoi na https://localhost:8443/nifi

Usera i hasło trzeba z logów dockera wydobyć, ale może ogarnę, by były jakieś zamokowane.


Kafka też już działa

Sprawdzenie, że kafka działa:

1. Attachujemy shella do kontenera z kafką
2. `kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092`
3. `kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092` i wpisujemy jakieś śmieci
4. `kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092` i voila!