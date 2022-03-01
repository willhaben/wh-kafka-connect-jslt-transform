package at.willhaben.kafka.connect.transforms.jslt

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import java.util.*
import kotlin.test.Test
import kotlin.test.assertTrue


class JsltTransformTest {
    private val xformKey: JsltTransform<SourceRecord> = JsltTransform.Key()
    private val xformValue: JsltTransform<SourceRecord> = JsltTransform.Value()

    @AfterEach
    fun teardown() {
        xformKey.close()
        xformValue.close()
    }

    fun configureNonTransformJslt(transform: JsltTransform<SourceRecord>) {
        val props: MutableMap<String, String> = HashMap()
        props["jslt"] = "."
        transform.configure(props.toMap())
    }

    @Test
    fun topLevelStructRequired() {
        configureNonTransformJslt(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, Schema.INT32_SCHEMA, 42
                )
            )
        }
    }

    @Test
    fun topLevelMapRequired() {
        configureNonTransformJslt(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, null, 42
                )
            )
        }
    }

    @Test
    fun testNestedStruct() {
        configureNonTransformJslt(xformValue)
        var builder = SchemaBuilder.struct()
        builder.field("int8", Schema.INT8_SCHEMA)
        builder.field("int16", Schema.INT16_SCHEMA)
        builder.field("int32", Schema.INT32_SCHEMA)
        builder.field("int64", Schema.INT64_SCHEMA)
        builder.field("float32", Schema.FLOAT32_SCHEMA)
        builder.field("float64", Schema.FLOAT64_SCHEMA)
        builder.field("boolean", Schema.BOOLEAN_SCHEMA)
        builder.field("string", Schema.STRING_SCHEMA)
        builder.field("bytes", Schema.BYTES_SCHEMA)
        val supportedTypesSchema = builder.build()
        builder = SchemaBuilder.struct()
        builder.field("B", supportedTypesSchema)
        val oneLevelNestedSchema = builder.build()
        builder = SchemaBuilder.struct()
        builder.field("A", oneLevelNestedSchema)
        val twoLevelNestedSchema = builder.build()
        val supportedTypes = Struct(supportedTypesSchema)
        supportedTypes.put("int8", 8.toByte())
        supportedTypes.put("int16", 16.toShort())
        supportedTypes.put("int32", 32)
        supportedTypes.put("int64", 64.toLong())
        supportedTypes.put("float32", 32f)
        supportedTypes.put("float64", 64.0)
        supportedTypes.put("boolean", true)
        supportedTypes.put("string", "stringy")
        supportedTypes.put("bytes", "bytes".toByteArray())
        val oneLevelNestedStruct = Struct(oneLevelNestedSchema)
        oneLevelNestedStruct.put("B", supportedTypes)
        val twoLevelNestedStruct = Struct(twoLevelNestedSchema)
        twoLevelNestedStruct.put("A", oneLevelNestedStruct)
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                twoLevelNestedSchema, twoLevelNestedStruct
            )
        )
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
        val transformedStruct = transformed.value() as Struct
        assertEquals(9, transformedStruct.schema().fields().size)
        assertEquals(8, transformedStruct.getInt8("A.B.int8") as Byte)
        assertEquals(16, transformedStruct.getInt16("A.B.int16") as Short)
        assertEquals(32, transformedStruct.getInt32("A.B.int32") as Int)
        assertEquals(64L, transformedStruct.getInt64("A.B.int64") as Long)
        assertEquals(32f, transformedStruct.getFloat32("A.B.float32"), 0f)
        assertEquals(64.0, transformedStruct.getFloat64("A.B.float64"), 0.0)
        assertEquals(true, transformedStruct.getBoolean("A.B.boolean"))
        assertEquals("stringy", transformedStruct.getString("A.B.string"))
        assertArrayEquals("bytes".toByteArray(), transformedStruct.getBytes("A.B.bytes"))
    }

    @Test
    fun testNestedMapWithDelimiter() {
        configureNonTransformJslt(xformValue)
        val supportedTypes: MutableMap<String, Any> = HashMap()
        supportedTypes["int8"] = 8.toByte()
        supportedTypes["int16"] = 16.toShort()
        supportedTypes["int32"] = 32
        supportedTypes["int64"] = 64.toLong()
        supportedTypes["float32"] = 32f
        supportedTypes["float64"] = 64.0
        supportedTypes["boolean"] = true
        supportedTypes["string"] = "stringy"
        supportedTypes["bytes"] = "bytes".toByteArray()
        val oneLevelNestedMap = Collections.singletonMap<String, Any>("B", supportedTypes)
        val twoLevelNestedMap = Collections.singletonMap<String, Any>("A", oneLevelNestedMap)
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                null, twoLevelNestedMap
            )
        )
        assertNull(transformed.valueSchema())
        assertTrue(transformed.value() is Map<*, *>)
        val transformedMap = transformed.value() as Map<String, Any>
        assertEquals(9, transformedMap.size)
        assertEquals(8.toByte(), transformedMap["A#B#int8"])
        assertEquals(16.toShort(), transformedMap["A#B#int16"])
        assertEquals(32, transformedMap["A#B#int32"])
        assertEquals(64.toLong(), transformedMap["A#B#int64"])
        assertEquals(32f, transformedMap["A#B#float32"] as Float, 0f)
        assertEquals(64.0, transformedMap["A#B#float64"] as Double, 0.0)
        assertEquals(true, transformedMap["A#B#boolean"])
        assertEquals("stringy", transformedMap["A#B#string"])
        assertArrayEquals("bytes".toByteArray(), transformedMap["A#B#bytes"] as ByteArray?)
    }

    @Test
    fun testOptionalFieldStruct() {
        configureNonTransformJslt(xformValue)
        var builder = SchemaBuilder.struct()
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
        val supportedTypesSchema = builder.build()
        builder = SchemaBuilder.struct()
        builder.field("B", supportedTypesSchema)
        val oneLevelNestedSchema = builder.build()
        val supportedTypes = Struct(supportedTypesSchema)
        supportedTypes.put("opt_int32", null)
        val oneLevelNestedStruct = Struct(oneLevelNestedSchema)
        oneLevelNestedStruct.put("B", supportedTypes)
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                oneLevelNestedSchema, oneLevelNestedStruct
            )
        )
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
        val transformedStruct = transformed.value() as Struct
        assertNull(transformedStruct["B.opt_int32"])
    }

    @Test
    fun testOptionalStruct() {
        configureNonTransformJslt(xformValue)
        val builder = SchemaBuilder.struct().optional()
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
        val schema = builder.build()
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                schema, null
            )
        )
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
        assertNull(transformed.value())
    }

    @Test
    fun testOptionalNestedStruct() {
        configureNonTransformJslt(xformValue)
        var builder = SchemaBuilder.struct().optional()
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
        val supportedTypesSchema = builder.build()
        builder = SchemaBuilder.struct()
        builder.field("B", supportedTypesSchema)
        val oneLevelNestedSchema = builder.build()
        val oneLevelNestedStruct = Struct(oneLevelNestedSchema)
        oneLevelNestedStruct.put("B", null)
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                oneLevelNestedSchema, oneLevelNestedStruct
            )
        )
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
        val transformedStruct = transformed.value() as Struct
        assertNull(transformedStruct["B.opt_int32"])
    }

    @Test
    fun testOptionalFieldMap() {
        configureNonTransformJslt(xformValue)
        val supportedTypes: MutableMap<String, Any?> = HashMap()
        supportedTypes["opt_int32"] = null
        val oneLevelNestedMap = Collections.singletonMap<String, Any>("B", supportedTypes)
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                null, oneLevelNestedMap
            )
        )
        assertNull(transformed.valueSchema())
        assertTrue(transformed.value() is Map<*, *>)
        val transformedMap = transformed.value() as Map<String, Any>
        assertNull(transformedMap["B.opt_int32"])
    }

    @Test
    fun testKey() {
        configureNonTransformJslt(xformValue)
        val key = Collections.singletonMap("A", Collections.singletonMap("B", 12))
        val src = SourceRecord(null, null, "topic", null, key, null, null)
        val transformed: SourceRecord = xformKey.apply(src)
        assertNull(transformed.keySchema())
        assertTrue(transformed.key() is Map<*, *>)
        val transformedMap = transformed.key() as Map<String, Any>
        assertEquals(12, transformedMap["A.B"])
    }

    @Test
    fun testSchemalessArray() {
        configureNonTransformJslt(xformValue)
        val value: Any = Collections.singletonMap(
            "foo",
            Arrays.asList("bar", Collections.singletonMap("baz", Collections.singletonMap("lfg", "lfg")))
        )
        assertEquals(value, xformValue.apply(SourceRecord(null, null, "topic", null, null, null, value)).value())
    }

    @Test
    fun testArrayWithSchema() {
        configureNonTransformJslt(xformValue)
        val nestedStructSchema = SchemaBuilder.struct().field("lfg", Schema.STRING_SCHEMA).build()
        val innerStructSchema = SchemaBuilder.struct().field("baz", nestedStructSchema).build()
        val structSchema = SchemaBuilder.struct()
            .field("foo", SchemaBuilder.array(innerStructSchema).doc("durk").build())
            .build()
        val nestedValue = Struct(nestedStructSchema)
        nestedValue.put("lfg", "lfg")
        val innerValue = Struct(innerStructSchema)
        innerValue.put("baz", nestedValue)
        val value = Struct(structSchema)
        value.put("foo", listOf(innerValue))
        val transformed: SourceRecord =
            xformValue.apply(SourceRecord(null, null, "topic", null, null, structSchema, value))
        assertEquals(value, transformed.value())
        assertEquals(structSchema, transformed.valueSchema())
    }

    @Test
    fun testOptionalAndDefaultValuesNested() {
        // If we have a nested structure where an entire sub-Struct is optional, all flattened fields generated from its
        // children should also be optional. Similarly, if the parent Struct has a default value, the default value for
        // the flattened field
        configureNonTransformJslt(xformValue)
        val builder = SchemaBuilder.struct().optional()
        builder.field("req_field", Schema.STRING_SCHEMA)
        builder.field("opt_field", SchemaBuilder.string().optional().defaultValue("child_default").build())
        val childDefaultValue = Struct(builder)
        childDefaultValue.put("req_field", "req_default")
        builder.defaultValue(childDefaultValue)
        val schema = builder.build()
        // Intentionally leave this entire value empty since it is optional
        val value = Struct(schema)
        val transformed: SourceRecord = xformValue.apply(SourceRecord(null, null, "topic", 0, schema, value))
        assertNotNull(transformed)
        val transformedSchema = transformed.valueSchema()
        assertEquals(Schema.Type.STRUCT, transformedSchema.type())
        assertEquals(2, transformedSchema.fields().size)
        // Required field should pick up both being optional and the default value from the parent
        val transformedReqFieldSchema = SchemaBuilder.string().optional().defaultValue("req_default").build()
        assertEquals(transformedReqFieldSchema, transformedSchema.field("req_field").schema())
        // The optional field should still be optional but should have picked up the default value. However, since
        // the parent didn't specify the default explicitly, we should still be using the field's normal default
        val transformedOptFieldSchema = SchemaBuilder.string().optional().defaultValue("child_default").build()
        assertEquals(transformedOptFieldSchema, transformedSchema.field("opt_field").schema())
    }

    @Test
    fun tombstoneEventWithoutSchemaShouldPassThrough() {
        configureNonTransformJslt(xformValue)
        val record = SourceRecord(
            null, null, "test", 0,
            null, null
        )
        val transformedRecord: SourceRecord = xformValue.apply(record)
        assertNull(transformedRecord.value())
        assertNull(transformedRecord.valueSchema())
    }

    @Test
    fun tombstoneEventWithSchemaShouldPassThrough() {
        configureNonTransformJslt(xformValue)
        val simpleStructSchema =
            SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA)
                .build()
        val record = SourceRecord(
            null, null, "test", 0,
            simpleStructSchema, null
        )
        val transformedRecord: SourceRecord = xformValue.apply(record)
        assertNull(transformedRecord.value())
        assertEquals(simpleStructSchema, transformedRecord.valueSchema())
    }

    @Test
    fun testMapWithNullFields() {
        configureNonTransformJslt(xformValue)

        // Use a LinkedHashMap to ensure the SMT sees entries in a specific order
        val value: MutableMap<String, Any?> = LinkedHashMap()
        value["firstNull"] = null
        value["firstNonNull"] = "nonNull"
        value["secondNull"] = null
        value["secondNonNull"] = "alsoNonNull"
        value["thirdNonNull"] = null
        val record = SourceRecord(null, null, "test", 0, null, value)
        val transformedRecord: SourceRecord = xformValue.apply(record)
        assertEquals(value, transformedRecord.value())
    }

    @Test
    fun testStructWithNullFields() {
        configureNonTransformJslt(xformValue)
        val structSchema = SchemaBuilder.struct()
            .field("firstNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("firstNonNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("secondNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("secondNonNull", Schema.OPTIONAL_STRING_SCHEMA)
            .field("thirdNonNull", Schema.OPTIONAL_STRING_SCHEMA)
            .build()
        val value = Struct(structSchema)
        value.put("firstNull", null)
        value.put("firstNonNull", "nonNull")
        value.put("secondNull", null)
        value.put("secondNonNull", "alsoNonNull")
        value.put("thirdNonNull", null)
        val record = SourceRecord(null, null, "test", 0, structSchema, value)
        val transformedRecord: SourceRecord = xformValue.apply(record)
        assertEquals(value, transformedRecord.value())
    }
}