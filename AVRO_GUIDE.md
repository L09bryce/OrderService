# Avro and Kafka in Order Service

This guide explains the Avro serialization problem encountered while publishing
`OrderPlacedEvent`, the diagnostic changes made in `OrderService`, and the
complete producer flow.

## 1. What Avro is

Apache Avro is a data serialization system. It converts structured application
objects into a compact binary format for transport or storage, and converts the
binary data back into objects later.

Avro is schema-based. A schema describes:

- The record name
- The namespace
- The field names
- The field types
- Whether fields are required or nullable
- Optional default values

The schema used by this service is
`src/main/resources/avro/order-placed.avsc`:

```json
{
  "type": "record",
  "name": "OrderPlacedEvent",
  "namespace": "com.kiru.microservice.order.event",
  "fields": [
    { "name": "orderNumber", "type": "string" },
    { "name": "email", "type": "string" },
    { "name": "firstName", "type": "string" },
    { "name": "lastName", "type": "string" }
  ]
}
```

Every field is a required Avro string. A `null` value would not be valid for
this schema unless the field type were changed to a union such as
`["null", "string"]` with a default.

## 2. Avro versus JSON

The HTTP request and the Kafka event are two separate representations.

The client sends JSON to the Order Service:

```json
{
  "skuCode": "OnePlus",
  "quantity": 15,
  "userDetails": {
    "email": "fionawangeci@gmail.com",
    "firstName": "fiona",
    "lastName": "gichuhi"
  }
}
```

Spring MVC converts this JSON into the Java `OrderRequest` record. The service
then creates an `OrderPlacedEvent` and sends that Java object to Kafka.
`KafkaAvroSerializer` converts the Java event into Avro binary data internally.

Therefore, Postman should continue sending JSON. It must not send an Avro
binary payload to the HTTP endpoint.

## 3. The generated Java class

The Avro Maven plugin reads the `.avsc` file and generates:

```text
com.kiru.microservice.order.event.OrderPlacedEvent
```

The generated class implements `SpecificRecord`. It contains:

- A static `SCHEMA$` object containing the parsed Avro schema
- Getter and setter methods for the schema fields
- `getSchema()`
- Indexed `get(int)` and `put(int, Object)` methods used by Avro
- Avro-specific encoding and decoding support

The generated file is normally not edited manually. If the schema changes,
update the `.avsc` file and regenerate the class with Maven.

The generated source is located at:

```text
src/main/java/com/kiru/microservice/order/event/OrderPlacedEvent.java
```

The source is generated from:

```text
src/main/resources/avro/order-placed.avsc
```

## 4. The producer configuration

The relevant settings in `src/main/resources/application.properties` are:

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
spring.kafka.producer.properties.schema.registry.url=http://127.0.0.1:8085
```

These settings mean:

1. Connect to Kafka at `localhost:9092`.
2. Serialize Kafka record keys as strings.
3. Serialize Kafka record values using Confluent's Avro serializer.
4. Use Schema Registry at `http://127.0.0.1:8085`.

The Kafka message key is not explicitly provided in the current call, so the
event value is the important part for this problem.

## 5. What happens when an order is placed

The relevant code is in
`src/main/java/com/kiru/microservice/order/service/OrderService.java`.

The flow is:

1. Validate the incoming `OrderRequest`.
2. Ask Inventory Service whether the SKU is available.
3. Create and save the `Order` entity in MySQL.
4. Create an `OrderPlacedEvent`.
5. Copy the order number and user details into the event.
6. Call:

   ```java
   kafkaTemplate.send("order-placed-topic", orderPlacedEvent)
   ```

7. `KafkaTemplate` delegates to the Kafka producer.
8. `KafkaAvroSerializer` obtains the event schema from the generated
   `SpecificRecord`.
9. The serializer registers or looks up the schema in Schema Registry.
10. The event is encoded into Avro binary data.
11. Kafka receives the serialized record and publishes it to
    `order-placed-topic`.
12. A future completes when Kafka acknowledges or rejects the send.

The notification service is not required for steps 1 through 11. It is only
needed later to consume and process the published message.

## 6. Schema Registry

Schema Registry stores schemas separately from Kafka messages. Instead of
putting the complete schema into every message, the Confluent serializer
typically stores a schema ID in the serialized message and registers the
schema in Schema Registry.

The Docker configuration maps:

```text
host port 8085 -> Schema Registry container port 8081
```

That is why the application uses:

```text
http://127.0.0.1:8085
```

Useful checks:

```text
GET http://127.0.0.1:8085/subjects
GET http://127.0.0.1:8085/subjects/order-placed-topic-value/versions/latest
```

The subject is:

```text
order-placed-topic-value
```

The default `TopicNameStrategy` creates the subject by appending `-value` to
the Kafka topic name. The registered schema was:

```json
{
  "type": "record",
  "name": "OrderPlacedEvent",
  "namespace": "com.kiru.microservice.order.event",
  "fields": [
    { "name": "orderNumber", "type": "string" },
    { "name": "email", "type": "string" },
    { "name": "firstName", "type": "string" },
    { "name": "lastName", "type": "string" }
  ]
}
```

This matched the local schema, so Schema Registry connectivity and schema
compatibility were not the problem.

## 7. The error that occurred

The important nested exception was:

```text
java.lang.SecurityException:
Forbidden com.kiru.microservice.order.event.OrderPlacedEvent!
```

Avro 1.12 includes class-security validation. When Avro resolves a generated
specific record class while creating its datum writer, it checks whether the
class is trusted. The generated class was valid, but its package/class was not
on Avro's trusted allowlist.

This is a local JVM security check. It is not:

- A Postman content-type problem
- A missing notification consumer
- A Schema Registry availability problem
- A schema mismatch
- A Kafka broker authentication problem

## 8. The fix that was applied

The selected fix was the class-level allowlist:

```text
-Dorg.apache.avro.SERIALIZABLE_CLASSES=com.kiru.microservice.order.event.OrderPlacedEvent
```

This was added to the IntelliJ run configuration's VM options and the
application was restarted.

The option tells Avro that this exact generated class is trusted for schema
resolution and serialization.

Class-level trust is narrower than trusting the entire package. The alternative
would be:

```text
-Dorg.apache.avro.SERIALIZABLE_PACKAGES=com.kiru.microservice.order.event
```

That alternative trusts all classes in the event package. It can be useful
when the application has many generated event classes, but the class-level
option is more restrictive for the current single-event case.

The JVM option must be present on the process that runs the Order Service. It
does not belong in the HTTP request, Kafka message, Schema Registry, or
notification service configuration.

## 9. Diagnostic logging added

The Kafka send is asynchronous after the producer future is created, but
serialization can fail synchronously while `KafkaTemplate.send(...)` is
executing.

The service now handles both paths:

- A `whenComplete` callback logs asynchronous producer failures and successful
  partition/offset metadata.
- A surrounding `try/catch` logs synchronous serialization failures and
  rethrows them.

This distinction was important:

```text
kafkaTemplate.send(...)
```

can fail before returning a future. In that case, attaching
`whenComplete(...)` alone cannot observe the exception.

The diagnostic logs use:

```text
Kafka send failed synchronously for OrderPlacedEvent ...
```

for immediate failures, and:

```text
Failed to send OrderPlacedEvent ...
```

for failures delivered through the future.

Successful delivery logs the Kafka partition and offset.

## 10. What the consumer will need later

The notification service must use compatible Kafka and Avro configuration:

```properties
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.consumer.properties.schema.registry.url=http://127.0.0.1:8085
spring.kafka.consumer.properties.specific.avro.reader=true
```

The consumer should subscribe to:

```text
order-placed-topic
```

For a generated specific-record consumer, it also needs the compatible
`OrderPlacedEvent` class generated from the same schema, or an equivalent
reader schema that follows Avro compatibility rules.

The consumer does not need to be running for the producer to serialize and
publish an event. Kafka retains the event according to the topic's retention
configuration until a consumer reads it.

## 11. Schema evolution

When changing `OrderPlacedEvent`, update the `.avsc` file rather than editing
the generated Java file. Consider compatibility:

- Adding a required field without a default can break old consumers.
- Adding a field with a default is generally safer.
- Removing or renaming fields can break consumers.
- Changing a field type can be incompatible.
- Making a field nullable requires a union type and usually a default.

For example, a safer optional field looks like:

```json
{
  "name": "phoneNumber",
  "type": ["null", "string"],
  "default": null
}
```

Schema Registry compatibility settings should be checked before deploying
schema changes.

## 12. Useful troubleshooting sequence

When Avro publishing fails:

1. Confirm the HTTP request is still JSON.
2. Confirm all required event fields are non-null.
3. Inspect the complete nested exception, not only
   `Error serializing Avro message`.
4. Check that Kafka is reachable at `localhost:9092`.
5. Check Schema Registry at `http://127.0.0.1:8085/subjects`.
6. Inspect the registered subject and schema.
7. Confirm the producer value serializer is `KafkaAvroSerializer`.
8. Confirm the generated event class matches the `.avsc` schema.
9. Check Avro class-security configuration.
10. Check the Kafka send callback for asynchronous delivery failures.

## 13. Temporary cleanup

`AVRO_GUIDE.md` is documentation for this debugging session and may be
deleted when no longer needed.

The IntelliJ VM option should remain in the run configuration while this
application uses the current Avro setup:

```text
-Dorg.apache.avro.SERIALIZABLE_CLASSES=com.kiru.microservice.order.event.OrderPlacedEvent
```
