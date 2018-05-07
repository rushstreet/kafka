


# Quick Guide to Securely Sharing Secrets

## Table of Contents

1. [Introduction](#introduction)
2. [Importing into Eclipse](#importing-into-eclipse)
3. [Implementation](#implementation)

## Introduction

Single Message Transforms (SMT) were defined in [KIP-66](https://cwiki.apache.org/confluence/display/KAFKA/KIP-66%3A+Single+Message+Transforms+for+Kafka+Connect). An SMT is a Java class that implements the `Transformation` interface:

```
public interface Transformation<R extends ConnectRecord<R>> extends Configurable, Closeable {

    /**
     * Apply transformation to the {@code record} and return another record object (which may be {@code record} itself) or {@code null},
     * corresponding to a map or filter operation respectively.
     *
     * The implementation must be thread-safe.
     */
    R apply(R record);

    /** Configuration specification for this transformation. **/
    ConfigDef config();

    /** Signal that this transformation instance will no longer will be used. **/
    @Override
    void close();

}
```

`Transformation` is a generic interface that is parameterized by a class `R` that extends the generic abstract class `ConnectRecord`. The main thing to note about the `ConnectRecord` class is that it declares an abstract method `newRecord`, which returns a new record of the same type:

```
/**
 * Create a new record of the same type as itself, with the specified parameter values. All other fields in this record will be copied
 * over to the new record. Since the headers are mutable, the resulting record will have a copy of this record's headers.
 *
 * @param topic the name of the topic; may be null
 * @param kafkaPartition the partition number for the Kafka topic; may be null
 * @param keySchema the schema for the key; may be null
 * @param key the key; may be null
 * @param valueSchema the schema for the value; may be null
 * @param value the value; may be null
 * @param timestamp the timestamp; may be null
 * @return the new record
 */

public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp);
```

At the core when writing a custom SMT, you are implementing `R apply(R record)`, `ConfigDef config()`, and `void close()` from `Transformation`.

## Importing into Eclipse

For building the Kafka project, I referred to this guide from IBM: [How to Develop for Apache Kafka using eclipse](https://developer.ibm.com/opentech/2016/06/06/how-to-develop-for-apache-kafka-using-eclipse/). As a brief overview:

**1. Clone into the Kafka repository**

```
$ git clone https://github.com/rushstreet/kafka.git
```

**2. Use Gradle to build the project for Eclipse**

```
$ cd kafka/
$ gradle
$ ./gradlew jar
$ ./gradlew eclipse
```

**3. Import the project into Eclipse**

- *File > Import > General > Existing Projects into Workspace*
- Select the Kafka folder, check *Search for nested projects*, click *Select all*, then click *Finish*.

## Implementation

In the package explorer you should see the `transforms` project. Expand it and expand `src/main/java`. You will see the package `org.apache.kafka.connect.transforms`. This package contains all the stock transforms shipped with the Kafka project. You can use these as a template for your custom SMT. We will be putting our custom SMTs in the package `io.rushstreet.transforms`. As an example, we will walk through the implementation of the `RSGTransform` class inside `io.rushstreet.transforms`.

**1. Implement `void close()`**

Here you should do any cleanup required before your transform is done being used. We do nothing in this method, as is the case in many of the stock transforms:

```
@Override
public void close() {}
```

**2. Implement `ConfigDef config()`**

We need to define what configuration options we will accept, as well as how to parse the configuration. We do this by creating and instance of `ConfigDef`:

```
public static final String TIMESTAMP_FIELD = "timestamp.field";

public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TIMESTAMP_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for extract timestamp.");
```

As you can see, we have defined a key (`timestamp.field`) by which we will reference this particular configuration. We have also defined a type, a default value (`null`), an importance, and a description for this configuration. We then return this instance of `ConfigDef` from the `config()` method:

```
@Override
public ConfigDef config() {
    return new ConfigDef();
}
```

In order to parse the configuration supplied by the user, we will actually need to override another method `void configure(Map<String, ?> props)`, which `Transformation` inherits from `Configurable`:

```
@Override
public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    extractTimestampField = config.getString(TIMESTAMP_FIELD);
    dateMap = new HashMap<>();
}
```

**3. Implement `R apply(R record)`**

This is where the magic happens.

## Resources

- none