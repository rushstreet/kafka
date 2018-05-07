# Writing a Custom Single Message Transform

## Table of Contents

1. [Introduction](#introduction)
2. [Importing into Eclipse](#importing-into-eclipse)
3. [Implementation](#implementation)
4. [Exporting JAR](#exporting-jar)
5. [Resources](#resources)

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

This is where the magic happens. The end goal is to return a new record which is a copy of the original record, but with an updated schema and/or payload. We will use a helper method for this purpose:

```
protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
}
```

So our job in `apply` is to come up with `updatedSchema` and `updatedValue`:

```
@Override
public R apply(R record) {
    Schema updatedSchema = getOrBuildSchema(record);
    final Struct value = requireStruct(operatingValue(record), PURPOSE);
    final Struct updatedValue = new Struct(updatedSchema);
    
    for (Field field : value.schema().fields()) {
        Object origFieldValue = value.get(field);
        
        Schema.Type targetType;       
        if (origFieldValue instanceof Timestamp) {
            targetType = Schema.Type.STRING;
        } else if (origFieldValue instanceof BigDecimal) {
            if (((BigDecimal) origFieldValue).scale() <= 0)
                targetType = Schema.Type.INT64;
            else
                targetType = Schema.Type.FLOAT64;
        } else {
            targetType = null;
        }
               
        Object newFieldValue = targetType != null ? castValueToType(origFieldValue, targetType) : origFieldValue;
        updatedValue.put(updatedSchema.field(field.name().toUpperCase()), newFieldValue);
    }
    
    if (extractTimestampField != null)
        updatedValue.put(updatedSchema.field(extractTimestampField), new Date().getTime());
    
    return newRecord(record, updatedSchema, updatedValue);
}
```

To generate the new schema we use a helper method called `getOrBuildSchema`:

```
private Schema getOrBuildSchema(R record) {
    Struct value = requireStruct(operatingValue(record), PURPOSE);
    Schema valueSchema = operatingSchema(record);
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());

    for (Field field : valueSchema.fields()) {
        SchemaBuilder fieldBuilder;
        Object origFieldValue = value.get(field);
        
        if (origFieldValue instanceof Timestamp) {
            fieldBuilder = convertFieldType(Schema.Type.STRING);
        } else if (origFieldValue instanceof BigDecimal) {
            if (((BigDecimal) origFieldValue).scale() <= 0)
                fieldBuilder = convertFieldType(Schema.Type.INT64);
            else
                fieldBuilder = convertFieldType(Schema.Type.FLOAT64);
        } else {
            fieldBuilder = convertFieldType(field.schema().type());
        }
        
        if (field.schema().isOptional())
            fieldBuilder.optional();
        if (field.schema().defaultValue() != null)
            fieldBuilder.defaultValue(castValueToType(field.schema().defaultValue(), fieldBuilder.type()));
        builder.field(field.name().toUpperCase(), fieldBuilder.build());
    }

    if (extractTimestampField != null)
        builder.field(extractTimestampField, SchemaBuilder.int64().build());

    if (valueSchema.isOptional())
        builder.optional();
    if (valueSchema.defaultValue() != null)
        builder.defaultValue(castValueToType(valueSchema.defaultValue(), builder.type()));

    return builder.build();
}
```

The main idea here is that we are iterating over the fields in the original schema and creating a series of `FieldBuilders`. A `FieldBuilder` simply represents a field in the new schema and is constructed by invoking `SchemaBuilder convertFieldType(Schema.Type type)`:

```
private SchemaBuilder convertFieldType(Schema.Type type) {
    switch (type) {
        case INT8:
            return SchemaBuilder.int8();
        case INT16:
            return SchemaBuilder.int16();
        case INT32:
            return SchemaBuilder.int32();
        case INT64:
            return SchemaBuilder.int64();
        case FLOAT32:
            return SchemaBuilder.float32();
        case FLOAT64:
            return SchemaBuilder.float64();
        case BOOLEAN:
            return SchemaBuilder.bool();
        case STRING:
            return SchemaBuilder.string();
        default:
            throw new DataException("Unexpected type in Cast transformation: " + type);
    }
}
```

This just returns a type for the new field in the new schema. The base case in the inner for loop is to simply copy the original field schema over to the new schema with the same type:

```
fieldBuilder = convertFieldType(field.schema().type());
```

We construct a new `SchemaBuilder` by adding each `FieldBuilder` at the bottom of the main loop, and then finally return the new schema by calling `builder.build()`. So now that we have the new schema, we can return to `apply`:

```
@Override
public R apply(R record) {
    Schema updatedSchema = getOrBuildSchema(record);
    final Struct value = requireStruct(operatingValue(record), PURPOSE);
    final Struct updatedValue = new Struct(updatedSchema);
    
    for (Field field : value.schema().fields()) {
        Object origFieldValue = value.get(field);
        
        Schema.Type targetType;       
        if (origFieldValue instanceof Timestamp) {
            targetType = Schema.Type.STRING;
        } else if (origFieldValue instanceof BigDecimal) {
            if (((BigDecimal) origFieldValue).scale() <= 0)
                targetType = Schema.Type.INT64;
            else
                targetType = Schema.Type.FLOAT64;
        } else {
            targetType = null;
        }
               
        Object newFieldValue = targetType != null ? castValueToType(origFieldValue, targetType) : origFieldValue;
        updatedValue.put(updatedSchema.field(field.name().toUpperCase()), newFieldValue);
    }
    
    if (extractTimestampField != null)
        updatedValue.put(updatedSchema.field(extractTimestampField), new Date().getTime());
    
    return newRecord(record, updatedSchema, updatedValue);
}
```

We iterate over all the fields again and cast their values if the new schema requires them to be cast:

```
private static Object castValueToType(Object value, Schema.Type targetType) {
    try {
        if (value == null) return null;
    
        if (!(value instanceof BigDecimal || value instanceof Timestamp)) {
            Schema.Type inferredType = ConnectSchema.schemaType(value.getClass());
            if (inferredType == null) {
                throw new DataException("Cast transformation was passed a value of type " + value.getClass()
                        + " which is not supported by Connect's data API");
            }
            // Ensure the type we are trying to cast from is supported
            validCastType(inferredType, FieldType.INPUT);
        }
        
        switch (targetType) {
            case INT8:
                return castToInt8(value);
            case INT16:
                return castToInt16(value);
            case INT32:
                return castToInt32(value);
            case INT64:
                return castToInt64(value);
            case FLOAT32:
                return castToFloat32(value);
            case FLOAT64:
                return castToFloat64(value);
            case BOOLEAN:
                return castToBoolean(value);
            case STRING:
                return castToString(value);
            default:
                throw new DataException(targetType.toString() + " is not supported in the Cast transformation.");
        }
    } catch (NumberFormatException e) {
        throw new DataException("Value (" + value.toString() + ") was out of range for requested data type", e);
    }
}
```

We then put the new values in an `updatedValue` struct and return `newRecord(record, updatedSchema, updatedValue)`.

## Exporting JAR

1. Right click `transforms` in the package explorer.

2. *Export > Java > JAR file*

3. Check *transforms* and *Export generated class files and resources*

4. Select the export destination (this should be a location in your Kafka classpath)

5. Click *Finish*

## Resources

- [KIP-66](https://cwiki.apache.org/confluence/display/KAFKA/KIP-66%3A+Single+Message+Transforms+for+Kafka+Connect)
- [Kafka Documentation](https://kafka.apache.org/documentation/#connect_transforms)
- [IBM OpenTech - How to Develop for Apache Kafka using eclipse](https://developer.ibm.com/opentech/2016/06/06/how-to-develop-for-apache-kafka-using-eclipse/)