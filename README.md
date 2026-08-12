# kafka_streaming_hotel_reviews
Coursework for NYP PDC in Enterprise Data Management (2026)

## Dataflow

```
              Kafka Protocol               Kafka Protocol
+--------------+   produce   +--------------+   consume   +-------------+
| ksql-datagen | ----------> | Kafka Broker | ----------> |   ksqlDB    |
+--------------+             +--------------+             +-------------+
                                      ^
                                      |
                              Kafka Topic
```
