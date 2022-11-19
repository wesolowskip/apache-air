# apache-air

```docker compose up```

Nifi stoi na https://localhost:8443/nifi

User: admin
Password: adminadminadmin

Wybieramy "Upload template" (w panelu po lewej stronie), wskaujzmey template flow_template.xml z katalogu nifi w repo, uploadujemy. Potem przeciągamy z górnego paska "Template" i wybieramy FlowTemplate. Przydatne opcje:
- na procesorze można wybrać "Run once"
- jak się nam coś zakolejkuje w połączeniu można wybrać "List queue" i poooglądać jsony
- oczywście jest też empty queue


Kafka też już działa

Sprawdzenie, że kafka działa:

1. Attachujemy shella do kontenera z kafką
2. `kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092`
3. `kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092` i wpisujemy jakieś śmieci
4. `kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092` i voila!

Spark streaming też chyba działa, można się do niego dostać po zwykłym http http://localhost:8888/