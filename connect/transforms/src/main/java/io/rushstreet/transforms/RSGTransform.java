/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rushstreet.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.math.BigDecimal;
import java.sql.Timestamp;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class RSGTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Transforms numeric types to floats and dates to strings.";

    public static final String TIMESTAMP_FIELD = "timestamp.field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TIMESTAMP_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for extract timestamp.");
    
    private static final String PURPOSE = "process messages for data reporting";

    private static final Set<Schema.Type> SUPPORTED_CAST_TYPES = EnumSet.of(
            Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
                    Schema.Type.FLOAT32, Schema.Type.FLOAT64, Schema.Type.BOOLEAN, Schema.Type.STRING
    );

    private String extractTimestampField = null;
    private HashMap<String, Schema.Type> dateMap;

    @Override
    public void close() {}

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        extractTimestampField = config.getString(TIMESTAMP_FIELD);
        dateMap = new HashMap<>();
    }

    @Override
    public R apply(R record) {
        Schema updatedSchema = getOrBuildSchema(record);
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(updatedSchema);
        
        for (Field field : value.schema().fields()) {
            Object origFieldValue = value.get(field);
            
            if (origFieldValue == null && !dateMap.containsKey(field.name().toUpperCase()))
            	continue;
            
            Schema.Type targetType;       
            if (origFieldValue instanceof Timestamp) {
                //targetType = Schema.Type.INT64;
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

    private Schema getOrBuildSchema(R record) {
        Struct value = requireStruct(operatingValue(record), PURPOSE);
        Schema valueSchema = operatingSchema(record);

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
        for (Field field : valueSchema.fields()) {
            SchemaBuilder fieldBuilder;
            Object origFieldValue = value.get(field);
            if (origFieldValue == null) {
            	if (dateMap.containsKey(field.name().toUpperCase()))
            		fieldBuilder = convertFieldType(dateMap.get(field.name().toUpperCase()));
            	else
            		continue;
            } else if (origFieldValue instanceof Timestamp) {
                //fieldBuilder = convertFieldType(Schema.Type.INT64);
                fieldBuilder = convertFieldType(Schema.Type.STRING);
            } else if (origFieldValue instanceof BigDecimal) {
                if (((BigDecimal) origFieldValue).scale() <= 0)
                    fieldBuilder = convertFieldType(Schema.Type.INT64);
                else
                    fieldBuilder = convertFieldType(Schema.Type.FLOAT64);
            } else {
                fieldBuilder = convertFieldType(field.schema().type());
            }

            dateMap.put(field.name().toUpperCase(), fieldBuilder.type());
       
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

    private static byte castToInt8(Object value) {
        if (value instanceof Number)
            return ((Number) value).byteValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? (byte) 1 : (byte) 0;
        else if (value instanceof String)
            return Byte.parseByte((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static short castToInt16(Object value) {
        if (value instanceof Number)
            return ((Number) value).shortValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? (short) 1 : (short) 0;
        else if (value instanceof String)
            return Short.parseShort((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static int castToInt32(Object value) {
        if (value instanceof Number)
            return ((Number) value).intValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1 : 0;
        else if (value instanceof String)
            return Integer.parseInt((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static long castToInt64(Object value) {
        if (value instanceof Number)
            return ((Number) value).longValue();
        else if (value instanceof Timestamp)
            return ((Timestamp) value).getTime();
        else if (value instanceof Boolean)
            return ((boolean) value) ? (long) 1 : (long) 0;
        else if (value instanceof String)
            return Long.parseLong((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static float castToFloat32(Object value) {
        if (value instanceof Number)
            return ((Number) value).floatValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1.f : 0.f;
        else if (value instanceof String)
            return Float.parseFloat((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static double castToFloat64(Object value) {
        if (value instanceof Number)
            return ((Number) value).doubleValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1. : 0.;
        else if (value instanceof String)
            return Double.parseDouble((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static boolean castToBoolean(Object value) {
        if (value instanceof Number)
            return ((Number) value).longValue() != 0L;
        else if (value instanceof Boolean)
            return (Boolean) value;
        else if (value instanceof String)
            return Boolean.parseBoolean((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static String castToString(Object value) {
        if (value instanceof Timestamp) {
            Timestamp ts = ((Timestamp) value);
            Date date = new Date(ts.getTime());
            return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
        } else {
            return value.toString();
        }
    }

    private enum FieldType {
        INPUT, OUTPUT
    }

    private static Schema.Type validCastType(Schema.Type type, FieldType fieldType) {
        if (!SUPPORTED_CAST_TYPES.contains(type)) {
            String message = "Cast transformation does not support casting to/from " + type
                    + "; supported types are " + SUPPORTED_CAST_TYPES;
            switch (fieldType) {
                case INPUT:
                    throw new DataException(message);
                case OUTPUT:
                    throw new ConfigException(message);
            }
        }
        return type;
    }
    
    protected Schema operatingSchema(R record) {
        return record.valueSchema();
    }

    protected Object operatingValue(R record) {
        return record.value();
    }

    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

}
