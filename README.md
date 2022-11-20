# apache-air

```docker compose up```

Nifi stoi na https://localhost:8443/nifi

User: admin
Password: adminadminadmin

Docker compose up automatycznie podłaczy nifi do kontenra nifi-registry. Nifi zaciągnie odpowiedni flow (skrypt do tego to `nifi/flow_setup.sh`).
Jesli chcemy wprowadzić zmiany we flowie to robimy:
- w Nifi mamy jeden główny bloczek z całym flowem (`FlowPG`), wchodzimy w niego i dostajemy dwa bloczki, które dotyczą powietrza i pogody
- Tutaj wprowadzamy zmiany, jakie chcemy
- Wracamy w górę (strzałka w górę w lewnym panelu), tak by widzieć tylko 1 bloczek na planszy. Klikamy kontrola wersji na nim, komitujemy
- W skrypcie `nifi/flow_setup.sh` podbijamy wersję w jsonie `VERSION_CONTROL`, by po postawieniu kontener ładował odpowiednią wersję

Szczegóły wersji można sprawdzić na http://localhost:18080/nifi-registry (tu stoi repo z flowem). Pamiętamy, by na gita zakomitować pliki z katalogu `nifi-registry`!!!!


Kafka też już działa

Sprawdzenie, że kafka działa:

1. Attachujemy shella do kontenera z kafką
2. `kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092`
3. `kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092` i wpisujemy jakieś śmieci
4. `kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092` i voila!

Spark streaming też chyba działa, można się do niego dostać po zwykłym http http://localhost:8888/

Hadoop namenode:
  https://localhost:9870
Hadoop datanode:
  https://localhost:9864
