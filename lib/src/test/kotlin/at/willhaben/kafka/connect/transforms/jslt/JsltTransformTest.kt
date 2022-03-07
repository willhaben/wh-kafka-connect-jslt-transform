package at.willhaben.kafka.connect.transforms.jslt

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import java.util.*
import kotlin.collections.HashMap
import kotlin.test.Test
import kotlin.test.assertNull
import kotlin.test.assertTrue


class JsltTransformTest {
    private val xformKey: JsltTransform<SourceRecord> = JsltTransform.Key()
    private val xformValue: JsltTransform<SourceRecord> = JsltTransform.Value()

    @AfterEach
    fun teardown() {
        xformKey.close()
        xformValue.close()
    }

    fun configureNonTransformJslt(transform: JsltTransform<SourceRecord>, jslt: String = ".") {
        val props: MutableMap<String, String> = HashMap()
        props["jslt"] = jslt
        transform.configure(props.toMap())
    }

    @Test
    fun handlesNullValue() {
        configureNonTransformJslt(xformValue)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformValue.apply(given).value()
        assertEquals(expected, actual)
    }

    @Test
    fun handlesNullKey() {
        configureNonTransformJslt(xformKey)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformKey.apply(given).key()
        assertEquals(expected, actual)
    }

    @Test
    fun handlesSchemalessValue() {
        configureNonTransformJslt(xformValue)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null,
            null,
            mapOf(
                "numberField" to Int.MAX_VALUE,
                "floatField" to Float.MAX_VALUE,
                "stringField" to "someString",
                "booleanField" to true,
                "recordField" to mapOf("valueA" to 123456, "valueB" to "xxx"),
                "arrayField" to arrayOf("ElemA", "ElemB", "ElemC"),
                "bytes" to "byteArray".toByteArray()
            )
            )

        val recordFieldSchema = SchemaBuilder
            .struct()
            .field("valueA", Schema.INT32_SCHEMA)
            .field("valueB", Schema.STRING_SCHEMA)
            .build()

        val expectedSchema = SchemaBuilder
                    .struct()
        .field("numberField", Schema.INT32_SCHEMA)
        .field("floatField", Schema.FLOAT32_SCHEMA)
        .field("stringField", Schema.STRING_SCHEMA)
            .field("booleanField", Schema.BOOLEAN_SCHEMA)
        .field("recordField", recordFieldSchema)
            .field("arrayField", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("bytes", Schema.BYTES_SCHEMA)
        .build()

        val expectedRecordField = Struct(recordFieldSchema)
        expectedRecordField.put("valueA", 123456)
        expectedRecordField.put("valueB", "xxx")

        val expected = Struct(expectedSchema)
        .put("numberField", Int.MAX_VALUE)
        .put("floatField", Float.MAX_VALUE)
        .put("stringField", "someString")
        .put("booleanField", true)
        .put("recordField", expectedRecordField)
        .put("arrayField", arrayOf("ElemA", "ElemB", "ElemC").toList())
        .put("bytes", "byteArray".toByteArray())

        val actual: Struct = xformValue.apply(given).value() as Struct
        assertEquals(expected.getInt32("numberField"), actual.getInt32("numberField"))
        assertEquals(expected.getFloat32("floatField"), actual.getFloat32("floatField"))
        assertEquals(expected.getString("stringField"), actual.getString("stringField"))
        assertEquals(expected.getBoolean("booleanField"), actual.getBoolean("booleanField"))
        assertEquals(expected.getStruct("recordField").getInt32("valueA"), actual.getStruct("recordField").getInt32("valueA"))
        assertEquals(expected.getStruct("recordField").getString("valueB"), actual.getStruct("recordField").getString("valueB"))
        assertEquals(expected.getArray<String>("arrayField"), actual.getArray<String>("arrayField"))
        assertEquals(expected.getBytes("bytes").toList(), actual.getBytes("bytes").toList())
    }

    @Test
    fun invalidValueForSchemalessValueRaisesDataException() {
        configureNonTransformJslt(xformValue)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            "someString....XXX"
        )
        assertThrows(DataException::class.java) {
            xformValue.apply(given).value()
        }
    }

    @Test
    fun hanldesSchemalessKey() {
        configureNonTransformJslt(xformKey)
        val key = Collections.singletonMap("A", Collections.singletonMap("B", 12))
        val src = SourceRecord(null, null, "topic", null, key, null, null)
        val transformed: SourceRecord = xformKey.apply(src)
        assertTrue(transformed.keySchema() is Schema)
        assertTrue(transformed.key() is Struct)
        val transformedMap = transformed.key() as Struct
        assertEquals(12, transformedMap.getStruct("A").getInt32("B"))
    }

    @Test
    fun convertsAllStructuredDataTypes() {
        configureNonTransformJslt(xformValue)

        val givenSubRecordFieldSchema = SchemaBuilder
            .struct()
            .field("int8Value", Schema.INT8_SCHEMA)
            .field("int16Value", Schema.INT16_SCHEMA)
            .field("int32Value", Schema.INT32_SCHEMA)
            .field("int64Value", Schema.INT64_SCHEMA)
            .field("stringValue", Schema.STRING_SCHEMA)
            .field("booleanValue", Schema.BOOLEAN_SCHEMA)
            .field("float32Value", Schema.FLOAT32_SCHEMA)
            .field("float64Value", Schema.FLOAT64_SCHEMA)
            .optional()
            .build()

        val givenRecordFieldSchema = SchemaBuilder
            .struct()
            .field("int32Value", Schema.INT32_SCHEMA)
            .field("stringValue", Schema.STRING_SCHEMA)
            .field("subRecord", givenSubRecordFieldSchema)
            .build()

        val givenSchema = SchemaBuilder
            .struct()
            .field("stringField", Schema.STRING_SCHEMA)
            .field("booleanField", Schema.BOOLEAN_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("nullField", Schema.OPTIONAL_STRING_SCHEMA)
            .field("recordField", givenRecordFieldSchema)
            .field("arrayOfRecordArray", SchemaBuilder.array(SchemaBuilder.array(givenRecordFieldSchema)))
            .field("bytes", Schema.BYTES_SCHEMA)
            .build()

        val givenSubRecord = Struct(givenSubRecordFieldSchema)
        .put("int8Value", Byte.MAX_VALUE)
        .put("int16Value", Short.MAX_VALUE)
        .put("int32Value", Int.MAX_VALUE)
        .put("int64Value", Long.MAX_VALUE)
        .put("stringValue", "xxx")
        .put("booleanValue", true)
        .put("float32Value", Float.MAX_VALUE)
        .put("float64Value", Float.MAX_VALUE+1.0)

        val givenSubRecord2 = Struct(givenSubRecordFieldSchema)
        .put("int8Value", Byte.MIN_VALUE)
        .put("int16Value", Short.MIN_VALUE)
        .put("int32Value", Int.MIN_VALUE)
        .put("int64Value", Long.MIN_VALUE)
        .put("stringValue", "xyz")
        .put("booleanValue", false)
        .put("float32Value", Float.MIN_VALUE)
        .put("float64Value", Float.MIN_VALUE-1.0)

        val givenRecord = Struct(givenRecordFieldSchema)
        .put("int32Value", Int.MIN_VALUE)
        .put("stringValue", "zzz")
        .put("subRecord", givenSubRecord)

        val givenArrayRecord1 = Struct(givenRecordFieldSchema)
        .put("int32Value", Int.MAX_VALUE)
        .put("stringValue", "yyy")

        val givenArrayRecord2 = Struct(givenRecordFieldSchema)
        .put("int32Value", Int.MIN_VALUE)
        .put("stringValue", "abc")

        val givenArrayRecord3 = Struct(givenRecordFieldSchema)
            .put("int32Value", 987)
            .put("stringValue", "cab")

        val givenValue = Struct(givenSchema)
            .put("stringField", "StringValue")
            .put("booleanField", true)
            .put("int8", 12.toByte())
            .put("int16", 34.toShort())
            .put("int32", 56)
            .put("int64", 78L)
            .put("float32", 1.23f)
            .put("float64", 4.56)
            .put("nullField", null)
            .put("recordField", givenRecord)
            .put("arrayOfRecordArray", arrayOf(arrayOf(givenArrayRecord1, givenArrayRecord2).toList(), arrayOf(givenArrayRecord3).toList()).toList())
            .put("bytes", "ExampleBytes".toByteArray())

        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null,
            givenSchema,
            givenValue
        )

        val expectedSubRecordFieldSchema = SchemaBuilder
            .struct()
            .field("int8Value", Schema.INT8_SCHEMA)
            .field("int16Value", Schema.INT16_SCHEMA)
            .field("int32Value", Schema.INT32_SCHEMA)
            .field("int64Value", Schema.INT64_SCHEMA)
            .field("stringValue", Schema.STRING_SCHEMA)
            .field("booleanValue", Schema.BOOLEAN_SCHEMA)
            .field("float32Value", Schema.FLOAT32_SCHEMA)
            .field("float64Value", Schema.FLOAT64_SCHEMA)
            .build()

        val expectedRecordFieldSchema = SchemaBuilder
            .struct()
            .field("int32Value", Schema.INT32_SCHEMA)
            .field("stringValue", Schema.STRING_SCHEMA)
            .field("subRecord", expectedSubRecordFieldSchema)
            .build()

        val expectedSchema = SchemaBuilder
            .struct()
            .field("stringField", Schema.STRING_SCHEMA)
            .field("booleanField", Schema.BOOLEAN_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("nullField", Schema.OPTIONAL_STRING_SCHEMA)
            .field("recordField", expectedRecordFieldSchema)
            .field("arrayOfRecordArray", SchemaBuilder.array(SchemaBuilder.array(expectedRecordFieldSchema)))
            .field("bytes", Schema.BYTES_SCHEMA)
            .build()

        val expectedSubRecord = Struct(expectedSubRecordFieldSchema)
            .put("int8Value", Byte.MAX_VALUE)
            .put("int16Value", Short.MAX_VALUE)
            .put("int32Value", Int.MAX_VALUE)
            .put("int64Value", Long.MAX_VALUE)
            .put("stringValue", "xxx")
            .put("booleanValue", true)
            .put("float32Value", Float.MAX_VALUE)
            .put("float64Value", Float.MAX_VALUE+1.0)

        val expectedSubRecord2 = Struct(expectedSubRecordFieldSchema)
            .put("int8Value", Byte.MIN_VALUE)
            .put("int16Value", Short.MIN_VALUE)
            .put("int32Value", Int.MIN_VALUE)
            .put("int64Value", Long.MIN_VALUE)
            .put("stringValue", "xyz")
            .put("booleanValue", false)
            .put("float32Value", Float.MIN_VALUE)
            .put("float64Value", Float.MIN_VALUE-1.0)

        val expectedRecord = Struct(expectedRecordFieldSchema)
            .put("int32Value", Int.MIN_VALUE)
            .put("stringValue", "zzz")
            .put("subRecord", expectedSubRecord)

        val expectedArrayRecord1 = Struct(expectedRecordFieldSchema)
            .put("int32Value", Int.MAX_VALUE)
            .put("stringValue", "yyy")
            .put("subRecord", expectedSubRecord)

        val expectedArrayRecord2 = Struct(expectedRecordFieldSchema)
            .put("int32Value", Int.MIN_VALUE)
            .put("stringValue", "abc")
            .put("subRecord", expectedSubRecord2)

        val expectedArrayRecord3 = Struct(expectedRecordFieldSchema)
            .put("int32Value", 987)
            .put("stringValue", "cab")
            .put("subRecord", expectedSubRecord)

        val expected = Struct(expectedSchema)
            .put("stringField", "StringValue")
            .put("booleanField", true)
            .put("int8", 12.toByte())
            .put("int16", 34.toShort())
            .put("int32", 56)
            .put("int64", 78L)
            .put("float32", 1.23f)
            .put("float64", 4.56)
            .put("recordField", expectedRecord)
            .put("arrayOfRecordArray", arrayOf(arrayOf(expectedArrayRecord1, expectedArrayRecord2).toList(), arrayOf(expectedArrayRecord3).toList()).toList())
            .put("bytes", "ExampleBytes".toByteArray())


        val actual: Struct = xformValue.apply(given).value() as Struct
        assertEquals(expected.getString("stringField"), actual.getString("stringField"))
        assertEquals(expected.getBoolean("booleanField"), actual.getBoolean("booleanField"))
        assertEquals(expected.getInt8("int8").toByte(), actual.getInt32("int8").toByte())
        assertEquals(expected.getInt16("int16").toShort(), actual.getInt32("int16").toShort())
        assertEquals(expected.getInt32("int32"), actual.getInt32("int32"))
        assertEquals(expected.getInt64("int64").toLong(), actual.getInt32("int64").toLong())
        assertEquals(expected.getFloat32("float32").toDouble(), actual.getFloat64("float32"), 0.0001)
        assertEquals(expected.getFloat64("float64"), actual.getFloat64("float64"), 0.0001)
        assertEquals(expected.getString("nullField"), actual.getString("nullField"))

        val actualRecordField = actual.getStruct("recordField")
        assertEquals(expectedRecord.getInt32("int32Value"), actualRecordField.getInt32("int32Value"))
        assertEquals(expectedRecord.getString("stringValue"), actualRecordField.getString("stringValue"))
        val actualSubRecordField = actualRecordField.getStruct("subRecord")
        assertEquals(expectedSubRecord.getInt8("int8Value").toInt(), actualSubRecordField.getInt32("int8Value"))
        assertEquals(expectedSubRecord.getInt16("int16Value").toInt(), actualSubRecordField.getInt32("int16Value"))
        assertEquals(expectedSubRecord.getInt32("int32Value"), actualSubRecordField.getInt32("int32Value"))
        assertEquals(expectedSubRecord.getInt64("int64Value"), actualSubRecordField.getInt64("int64Value"))
        assertEquals(expectedSubRecord.getString("stringValue"), actualSubRecordField.getString("stringValue"))
        assertEquals(expectedSubRecord.getBoolean("booleanValue"), actualSubRecordField.getBoolean("booleanValue"))
        assertEquals(expectedSubRecord.getFloat32("float32Value").toString(), actualSubRecordField.getFloat64("float32Value").toString())
        assertEquals(expectedSubRecord.getFloat64("float64Value"), actualSubRecordField.getFloat64("float64Value"), 0.001)

        val actualArrayOfRecordArray = actual.getArray<List<Struct>>("arrayOfRecordArray")
        assertEquals(2, actualArrayOfRecordArray.size)
        assertEquals(2, actualArrayOfRecordArray[0].size)
        assertEquals(1, actualArrayOfRecordArray[1].size)
        val actualArrayOfRecordArrayItem1 = actualArrayOfRecordArray[0][0]
        val actualArrayOfRecordArrayItem2 = actualArrayOfRecordArray[0][1]
        val actualArrayOfRecordArrayItem3 = actualArrayOfRecordArray[1][0]
        assertEquals(expectedArrayRecord1.getInt32("int32Value"), actualArrayOfRecordArrayItem1.getInt32("int32Value"))
        assertEquals(expectedArrayRecord1.getString("stringValue"), actualArrayOfRecordArrayItem1.getString("stringValue"))
        assertEquals(expectedArrayRecord2.getInt32("int32Value"), actualArrayOfRecordArrayItem2.getInt32("int32Value"))
        assertEquals(expectedArrayRecord2.getString("stringValue"), actualArrayOfRecordArrayItem2.getString("stringValue"))
        assertEquals(expectedArrayRecord3.getInt32("int32Value"), actualArrayOfRecordArrayItem3.getInt32("int32Value"))
        assertEquals(expectedArrayRecord3.getString("stringValue"), actualArrayOfRecordArrayItem3.getString("stringValue"))

    }

    @Test
    fun jsltTransformationIsApplied() {
        val jsltString = ""
        configureNonTransformJslt(xformValue)
        throw NotImplementedError()
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
//
//    @Test
//    fun testNestedStruct() {
//        configureNonTransformJslt(xformValue)
//        var builder = SchemaBuilder.struct()
//        builder.field("int8", Schema.INT8_SCHEMA)
//        builder.field("int16", Schema.INT16_SCHEMA)
//        builder.field("int32", Schema.INT32_SCHEMA)
//        builder.field("int64", Schema.INT64_SCHEMA)
//        builder.field("float32", Schema.FLOAT32_SCHEMA)
//        builder.field("float64", Schema.FLOAT64_SCHEMA)
//        builder.field("boolean", Schema.BOOLEAN_SCHEMA)
//        builder.field("string", Schema.STRING_SCHEMA)
//        builder.field("bytes", Schema.BYTES_SCHEMA)
//        val supportedTypesSchema = builder.build()
//        builder = SchemaBuilder.struct()
//        builder.field("B", supportedTypesSchema)
//        val oneLevelNestedSchema = builder.build()
//        builder = SchemaBuilder.struct()
//        builder.field("A", oneLevelNestedSchema)
//        val twoLevelNestedSchema = builder.build()
//        val supportedTypes = Struct(supportedTypesSchema)
//        supportedTypes.put("int8", Byte.MAX_VALUE)
//        supportedTypes.put("int16", Short.MAX_VALUE)
//        supportedTypes.put("int32", Int.MAX_VALUE)
//        supportedTypes.put("int64", Long.MAX_VALUE)
//        supportedTypes.put("float32", 32f)
//        supportedTypes.put("float64", 64.0)
//        supportedTypes.put("boolean", true)
//        supportedTypes.put("string", "stringy")
//        supportedTypes.put("bytes", "bytes".toByteArray())
//        val oneLevelNestedStruct = Struct(oneLevelNestedSchema)
//        oneLevelNestedStruct.put("B", supportedTypes)
//        val twoLevelNestedStruct = Struct(twoLevelNestedSchema)
//        twoLevelNestedStruct.put("A", oneLevelNestedStruct)
//        val transformed: SourceRecord = xformValue.apply(
//            SourceRecord(
//                null, null,
//                "topic", 0,
//                twoLevelNestedSchema, twoLevelNestedStruct
//            )
//        )
//        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
//        val transformedStruct = transformed.value() as Struct
//        val nestedStruct = ((transformed.value() as Struct).get("A") as Struct).get("B") as Struct
//        println(transformedStruct)
//        println(nestedStruct.get("bytes").javaClass.name)
//        assertEquals(9, nestedStruct.schema().fields().size)
//        assertEquals(Byte.MAX_VALUE.toInt(), nestedStruct.getInt32("int8") as Int)
//        assertEquals(Short.MAX_VALUE.toInt(), nestedStruct.getInt32("int16"))
//        assertEquals(Int.MAX_VALUE, nestedStruct.getInt32("int32"))
//        assertEquals(Long.MAX_VALUE, nestedStruct.get("int64") as Long)
//        assertEquals(32.0, nestedStruct.getFloat64("float32"), 0.0)
//        assertEquals(64.0, nestedStruct.getFloat64("float64"), 0.0)
//        assertEquals(true, nestedStruct.getBoolean("boolean"))
//        assertEquals("stringy", nestedStruct.getString("string"))
//        assertEquals(Base64.getEncoder().encodeToString("bytes".toByteArray()), nestedStruct.get("bytes"))
//    }
//
//    @Test
//    fun testNestedMapWithDelimiter() {
//        configureNonTransformJslt(xformValue)
//        val supportedTypes: MutableMap<String, Any> = HashMap()
//        supportedTypes["int8"] = 8.toByte()
//        supportedTypes["int16"] = 16.toShort()
//        supportedTypes["int32"] = 32
//        supportedTypes["int64"] = 64.toLong()
//        supportedTypes["float32"] = 32f
//        supportedTypes["float64"] = 64.0
//        supportedTypes["boolean"] = true
//        supportedTypes["string"] = "stringy"
//        supportedTypes["bytes"] = "bytes".toByteArray()
//        val oneLevelNestedMap = Collections.singletonMap<String, Any>("B", supportedTypes)
//        val twoLevelNestedMap = Collections.singletonMap<String, Any>("A", oneLevelNestedMap)
//        val transformed: SourceRecord = xformValue.apply(
//            SourceRecord(
//                null, null,
//                "topic", 0,
//                null, twoLevelNestedMap
//            )
//        )
//        assertNull(transformed.valueSchema())
//        assertTrue(transformed.value() is Map<*, *>)
//        val transformedMap = transformed.value() as Map<String, Any>
//        assertEquals(9, transformedMap.size)
//        assertEquals(8.toByte(), transformedMap["A#B#int8"])
//        assertEquals(16.toShort(), transformedMap["A#B#int16"])
//        assertEquals(32, transformedMap["A#B#int32"])
//        assertEquals(64.toLong(), transformedMap["A#B#int64"])
//        assertEquals(32f, transformedMap["A#B#float32"] as Float, 0f)
//        assertEquals(64.0, transformedMap["A#B#float64"] as Double, 0.0)
//        assertEquals(true, transformedMap["A#B#boolean"])
//        assertEquals("stringy", transformedMap["A#B#string"])
//        assertArrayEquals("bytes".toByteArray(), transformedMap["A#B#bytes"] as ByteArray?)
//    }
//
//    @Test
//    fun testOptionalFieldStruct() {
//        configureNonTransformJslt(xformValue)
//        var builder = SchemaBuilder.struct()
//        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
//        val supportedTypesSchema = builder.build()
//        builder = SchemaBuilder.struct()
//        builder.field("B", supportedTypesSchema)
//        val oneLevelNestedSchema = builder.build()
//        val supportedTypes = Struct(supportedTypesSchema)
//        supportedTypes.put("opt_int32", null)
//        val oneLevelNestedStruct = Struct(oneLevelNestedSchema)
//        oneLevelNestedStruct.put("B", supportedTypes)
//        val transformed: SourceRecord = xformValue.apply(
//            SourceRecord(
//                null, null,
//                "topic", 0,
//                oneLevelNestedSchema, oneLevelNestedStruct
//            )
//        )
//        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
//        val transformedStruct = transformed.value() as Struct
//        assertNull(transformedStruct["B.opt_int32"])
//    }
//
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
//
//    @Test
//    fun testOptionalNestedStruct() {
//        configureNonTransformJslt(xformValue)
//        var builder = SchemaBuilder.struct().optional()
//        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
//        val supportedTypesSchema = builder.build()
//        builder = SchemaBuilder.struct()
//        builder.field("B", supportedTypesSchema)
//        val oneLevelNestedSchema = builder.build()
//        val oneLevelNestedStruct = Struct(oneLevelNestedSchema)
//        oneLevelNestedStruct.put("B", null)
//        val transformed: SourceRecord = xformValue.apply(
//            SourceRecord(
//                null, null,
//                "topic", 0,
//                oneLevelNestedSchema, oneLevelNestedStruct
//            )
//        )
//        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
//        val transformedStruct = transformed.value() as Struct
//        assertNull(transformedStruct["B.opt_int32"])
//    }
//
//    @Test
//    fun testOptionalFieldMap() {
//        configureNonTransformJslt(xformValue)
//        val supportedTypes: MutableMap<String, Any?> = HashMap()
//        supportedTypes["opt_int32"] = null
//        val oneLevelNestedMap = Collections.singletonMap<String, Any>("B", supportedTypes)
//        val transformed: SourceRecord = xformValue.apply(
//            SourceRecord(
//                null, null,
//                "topic", 0,
//                null, oneLevelNestedMap
//            )
//        )
//        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
//        val transformedMap = transformed.value() as Struct
//        assertNull(transformedMap["B.opt_int32"])
//    }
//

//
//    @Test
//    fun testSchemalessArray() {
//        configureNonTransformJslt(xformValue)
//        val value: Any = Collections.singletonMap(
//            "foo",
//            Arrays.asList("bar", Collections.singletonMap("baz", Collections.singletonMap("lfg", "lfg")))
//        )
//        assertEquals(value, xformValue.apply(SourceRecord(null, null, "topic", null, null, null, value)).value())
//    }
//
//    @Test
//    fun testArrayWithSchema() {
//        configureNonTransformJslt(xformValue)
//        val nestedStructSchema = SchemaBuilder.struct().field("lfg", Schema.STRING_SCHEMA).build()
//        val innerStructSchema = SchemaBuilder.struct().field("baz", nestedStructSchema).build()
//        val structSchema = SchemaBuilder.struct()
//            .field("foo", SchemaBuilder.array(innerStructSchema).doc("durk").build())
//            .build()
//        val nestedValue = Struct(nestedStructSchema)
//        nestedValue.put("lfg", "lfg")
//        val innerValue = Struct(innerStructSchema)
//        innerValue.put("baz", nestedValue)
//        val value = Struct(structSchema)
//        value.put("foo", listOf(innerValue))
//        val transformed: SourceRecord =
//            xformValue.apply(SourceRecord(null, null, "topic", null, null, structSchema, value))
//        assertEquals(value, transformed.value())
//        assertEquals(structSchema, transformed.valueSchema())
//    }
//
//    @Test
//    fun testOptionalAndDefaultValuesNested() {
//        // If we have a nested structure where an entire sub-Struct is optional, all flattened fields generated from its
//        // children should also be optional. Similarly, if the parent Struct has a default value, the default value for
//        // the flattened field
//        configureNonTransformJslt(xformValue)
//        val builder = SchemaBuilder.struct().optional()
//        builder.field("req_field", Schema.STRING_SCHEMA)
//        builder.field("opt_field", SchemaBuilder.string().optional().defaultValue("child_default").build())
//        val childDefaultValue = Struct(builder)
//        childDefaultValue.put("req_field", "req_default")
//        builder.defaultValue(childDefaultValue)
//        val schema = builder.build()
//        // Intentionally leave this entire value empty since it is optional
//        val value = Struct(schema)
//        val transformed: SourceRecord = xformValue.apply(SourceRecord(null, null, "topic", 0, schema, value))
//        assertNotNull(transformed)
//        val transformedSchema = transformed.valueSchema()
//        assertEquals(Schema.Type.STRUCT, transformedSchema.type())
//        assertEquals(2, transformedSchema.fields().size)
//        // Required field should pick up both being optional and the default value from the parent
//        val transformedReqFieldSchema = SchemaBuilder.string().optional().defaultValue("req_default").build()
//        assertEquals(transformedReqFieldSchema, transformedSchema.field("req_field").schema())
//        // The optional field should still be optional but should have picked up the default value. However, since
//        // the parent didn't specify the default explicitly, we should still be using the field's normal default
//        val transformedOptFieldSchema = SchemaBuilder.string().optional().defaultValue("child_default").build()
//        assertEquals(transformedOptFieldSchema, transformedSchema.field("opt_field").schema())
//    }
//
//    @Test
//    fun tombstoneEventWithoutSchemaShouldPassThrough() {
//        configureNonTransformJslt(xformValue)
//        val record = SourceRecord(
//            null, null, "test", 0,
//            null, null
//        )
//        val transformedRecord: SourceRecord = xformValue.apply(record)
//        assertNull(transformedRecord.value())
//        assertNull(transformedRecord.valueSchema())
//    }
//
//    @Test
//    fun tombstoneEventWithSchemaShouldPassThrough() {
//        configureNonTransformJslt(xformValue)
//        val simpleStructSchema =
//            SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA)
//                .build()
//        val record = SourceRecord(
//            null, null, "test", 0,
//            simpleStructSchema, null
//        )
//        val transformedRecord: SourceRecord = xformValue.apply(record)
//        assertNull(transformedRecord.value())
//        assertEquals(simpleStructSchema, transformedRecord.valueSchema())
//    }
//
//    @Test
//    fun testMapWithNullFields() {
//        configureNonTransformJslt(xformValue)
//
//        // Use a LinkedHashMap to ensure the SMT sees entries in a specific order
//        val value: MutableMap<String, Any?> = LinkedHashMap()
//        value["firstNull"] = null
//        value["firstNonNull"] = "nonNull"
//        value["secondNull"] = null
//        value["secondNonNull"] = "alsoNonNull"
//        value["thirdNonNull"] = null
//        val record = SourceRecord(null, null, "test", 0, null, value)
//        val transformedRecord: SourceRecord = xformValue.apply(record)
//        assertEquals(value, transformedRecord.value())
//    }
//
//    @Test
//    fun testStructWithNullFields() {
//        configureNonTransformJslt(xformValue)
//        val structSchema = SchemaBuilder.struct()
//            .field("firstNull", Schema.OPTIONAL_STRING_SCHEMA)
//            .field("firstNonNull", Schema.OPTIONAL_STRING_SCHEMA)
//            .field("secondNull", Schema.OPTIONAL_STRING_SCHEMA)
//            .field("secondNonNull", Schema.OPTIONAL_STRING_SCHEMA)
//            .field("thirdNonNull", Schema.OPTIONAL_STRING_SCHEMA)
//            .build()
//        val value = Struct(structSchema)
//        value.put("firstNull", null)
//        value.put("firstNonNull", "nonNull")
//        value.put("secondNull", null)
//        value.put("secondNonNull", "alsoNonNull")
//        value.put("thirdNonNull", null)
//        val record = SourceRecord(null, null, "test", 0, structSchema, value)
//        val transformedRecord: SourceRecord = xformValue.apply(record)
//        assertEquals(value, transformedRecord.value())
//    }
}