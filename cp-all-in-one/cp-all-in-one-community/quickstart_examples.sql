[confluent terminal]
ksql-datagen quickstart=orders topic=orders_topic

[ksql terminal]
CREATE STREAM orders_raw (
    orderid varchar key,
    orderunits double as price,
    address struct <
        city varchar,
        state varchar,
        zipcode int >,
        ordertime VARCHAR
) WITH (
    KAFKA_TOPIC=’orders_topic’,
    VALUE_FORMAT=’JSON’
);

CREATE TABLE quickstart