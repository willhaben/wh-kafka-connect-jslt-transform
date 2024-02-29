package at.willhaben.kafka.connect.transforms.jslt

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import java.util.*
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue


class JsltTransformTest {
    private val xformKey: JsltTransform<SourceRecord> = JsltTransform.Key()
    private val xformValue: JsltTransform<SourceRecord> = JsltTransform.Value()
    private val objectMapper = ObjectMapper()

    @AfterEach
    fun teardown() {
        xformKey.close()
        xformValue.close()
    }

    fun configureNonTransformJslt(transform: JsltTransform<SourceRecord>, jslt: String = ".", schemaless: Boolean = false) {
        val props: MutableMap<String, String> = HashMap()
        props["jslt"] = jslt
        if(schemaless)
            props["jslt.schemaless"] = schemaless.toString()
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
    @Suppress("LongMethod")
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

        val actual: JsonNode = objectMapper.readValue(xformValue.apply(given).value() as String, JsonNode::class.java)
        assertEquals(expected.getInt32("numberField"), actual.get("numberField").asInt())
        assertEquals(expected.getFloat32("floatField"), actual.get("floatField").asDouble().toFloat())
        assertEquals(expected.getString("stringField"), actual.get("stringField").asText())
        assertEquals(expected.getBoolean("booleanField"), actual.get("booleanField").asBoolean())
        assertEquals(
            expected.getStruct("recordField").getInt32("valueA"),
            actual.get("recordField").get("valueA").asInt()
        )
        assertEquals(
            expected.getStruct("recordField").getString("valueB"),
            actual.get("recordField").get("valueB").asText()
        )
        assertEquals(expected.getArray<String>("arrayField"),
            actual.get("arrayField").elements().asSequence().map { it.asText() }.toList())
        assertEquals(expected.getBytes("bytes").toList(), actual.get("bytes").binaryValue().toList())
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
        assertTrue(transformed.keySchema() == null)
        assertTrue(transformed.key() is String)
        val actual: JsonNode = objectMapper.readValue(transformed.key() as String, JsonNode::class.java)
        assertEquals(12, actual.get("A").get("B").asInt())
    }

    @Test
    @Suppress("LongMethod")
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
            .put("float64Value", Float.MAX_VALUE + 1.0)

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
            .put(
                "arrayOfRecordArray",
                arrayOf(
                    arrayOf(givenArrayRecord1, givenArrayRecord2).toList(),
                    arrayOf(givenArrayRecord3).toList()
                ).toList()
            )
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
            .put("float64Value", Float.MAX_VALUE + 1.0)

        val expectedSubRecord2 = Struct(expectedSubRecordFieldSchema)
            .put("int8Value", Byte.MIN_VALUE)
            .put("int16Value", Short.MIN_VALUE)
            .put("int32Value", Int.MIN_VALUE)
            .put("int64Value", Long.MIN_VALUE)
            .put("stringValue", "xyz")
            .put("booleanValue", false)
            .put("float32Value", Float.MIN_VALUE)
            .put("float64Value", Float.MIN_VALUE - 1.0)

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
            .put(
                "arrayOfRecordArray",
                arrayOf(
                    arrayOf(expectedArrayRecord1, expectedArrayRecord2).toList(),
                    arrayOf(expectedArrayRecord3).toList()
                ).toList()
            )
            .put("bytes", "ExampleBytes".toByteArray())


        val actual: Struct = xformValue.apply(given).value() as Struct
        assertEquals(expected.getString("stringField"), actual.getString("stringField"))
        assertEquals(expected.getBoolean("booleanField"), actual.getBoolean("booleanField"))
        assertEquals(expected.getInt8("int8").toByte(), actual.getInt32("int8").toByte())
        assertEquals(expected.getInt16("int16").toShort(), actual.getInt32("int16").toShort())
        assertEquals(expected.getInt32("int32"), actual.getInt32("int32"))
        assertEquals(expected.getInt64("int64").toLong(), actual.getInt32("int64").toLong())
        assertEquals(
            expected.getFloat32("float32").toDouble(),
            actual.getFloat64("float32"), 0.0001
        )
        assertEquals(expected.getFloat64("float64"), actual.getFloat64("float64"), 0.0001)
        assertEquals(expected.getString("nullField"), actual.getString("nullField"))

        val actualRecordField = actual.getStruct("recordField")
        assertEquals(
            expectedRecord.getInt32("int32Value"),
            actualRecordField.getInt32("int32Value")
        )
        assertEquals(
            expectedRecord.getString("stringValue"),
            actualRecordField.getString("stringValue")
        )
        val actualSubRecordField = actualRecordField.getStruct("subRecord")
        assertEquals(
            expectedSubRecord.getInt8("int8Value").toInt(),
            actualSubRecordField.getInt32("int8Value")
        )
        assertEquals(
            expectedSubRecord.getInt16("int16Value").toInt(),
            actualSubRecordField.getInt32("int16Value")
        )
        assertEquals(
            expectedSubRecord.getInt32("int32Value"),
            actualSubRecordField.getInt32("int32Value")
        )
        assertEquals(
            expectedSubRecord.getInt64("int64Value"),
            actualSubRecordField.getInt64("int64Value")
        )
        assertEquals(
            expectedSubRecord.getString("stringValue"),
            actualSubRecordField.getString("stringValue")
        )
        assertEquals(
            expectedSubRecord.getBoolean("booleanValue"),
            actualSubRecordField.getBoolean("booleanValue")
        )
        assertEquals(
            expectedSubRecord.getFloat32("float32Value").toString(),
            actualSubRecordField.getFloat64("float32Value").toString()
        )
        assertEquals(
            expectedSubRecord.getFloat64("float64Value"),
            actualSubRecordField.getFloat64("float64Value"),
            0.001
        )

        val actualArrayOfRecordArray = actual.getArray<List<Struct>>("arrayOfRecordArray")
        assertEquals(2, actualArrayOfRecordArray.size)
        assertEquals(2, actualArrayOfRecordArray[0].size)
        assertEquals(1, actualArrayOfRecordArray[1].size)
        val actualArrayOfRecordArrayItem1 = actualArrayOfRecordArray[0][0]
        val actualArrayOfRecordArrayItem2 = actualArrayOfRecordArray[0][1]
        val actualArrayOfRecordArrayItem3 = actualArrayOfRecordArray[1][0]
        assertEquals(
            expectedArrayRecord1.getInt32("int32Value"),
            actualArrayOfRecordArrayItem1.getInt32("int32Value")
        )
        assertEquals(
            expectedArrayRecord1.getString("stringValue"),
            actualArrayOfRecordArrayItem1.getString("stringValue")
        )
        assertEquals(
            expectedArrayRecord2.getInt32("int32Value"),
            actualArrayOfRecordArrayItem2.getInt32("int32Value")
        )
        assertEquals(
            expectedArrayRecord2.getString("stringValue"),
            actualArrayOfRecordArrayItem2.getString("stringValue")
        )
        assertEquals(
            expectedArrayRecord3.getInt32("int32Value"),
            actualArrayOfRecordArrayItem3.getInt32("int32Value")
        )
        assertEquals(
            expectedArrayRecord3.getString("stringValue"),
            actualArrayOfRecordArrayItem3.getString("stringValue")
        )

    }

    @Test
    @Suppress("LongMethod")
    fun jsltTransformationIsApplied() {
        val jsltString = """
            let someConst = "constant_value"
            {
                "newConstField": ${"$"}someConst,
                "newConstField2": 1.23,
                "newNestedField": {
                    "numberField": .numberField,
                    "stringField": .stringField
                },
                "booleanField": .booleanField,
                "newArray": [ .inputNestedField.valueA, uppercase(.inputNestedField.valueB) ]
            }
        """.trimIndent()
        configureNonTransformJslt(xformValue, jsltString)

        val givenNestedSchema = SchemaBuilder
            .struct()
            .field("valueA", Schema.STRING_SCHEMA)
            .field("valueB", Schema.STRING_SCHEMA)
            .field("valueC", Schema.STRING_SCHEMA)
            .build()

        val givenSchema = SchemaBuilder
            .struct()
            .field("stringField", Schema.STRING_SCHEMA)
            .field("booleanField", Schema.BOOLEAN_SCHEMA)
            .field("numberField", Schema.INT16_SCHEMA)
            .field("inputNestedField", givenNestedSchema)
            .build()

        val givenNestedValue = Struct(givenNestedSchema)
            .put("valueA", "example_string_a")
            .put("valueB", "example_string_b")
            .put("valueC", "example_string_c")

        val givenValue = Struct(givenSchema)
            .put("stringField", "StringValue")
            .put("booleanField", true)
            .put("numberField", 1234.toShort())
            .put("inputNestedField", givenNestedValue)

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

        val expectedNestedSchema = SchemaBuilder
            .struct()
            .field("numberField", Schema.INT32_SCHEMA)
            .field("stringField", Schema.STRING_SCHEMA)
            .build()

        val expectedSchema = SchemaBuilder
            .struct()
            .field("newConstField", Schema.STRING_SCHEMA)
            .field("newConstField2", Schema.FLOAT64_SCHEMA)
            .field("newNestedField", expectedNestedSchema)
            .field("booleanField", Schema.BOOLEAN_SCHEMA)
            .field("newArray", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .build()

        val expectedNestedValue = Struct(expectedNestedSchema)
            .put("numberField", 1234)
            .put("stringField", "StringValue")

        val expected = Struct(expectedSchema)
            .put("newConstField", "constant_value")
            .put("newConstField2", 1.23)
            .put("newNestedField", expectedNestedValue)
            .put("booleanField", true)
            .put("newArray", arrayOf("example_string_a", "EXAMPLE_STRING_B").toList())

        val actual: Struct = xformValue.apply(given).value() as Struct

        assertEquals(expected.getString("newConstField"), actual.getString("newConstField"))
        assertEquals(expected.getFloat64("newConstField2"), actual.getFloat64("newConstField2"))
        assertEquals(expected.getBoolean("booleanField"), actual.getBoolean("booleanField"))
        assertEquals(
            expected.getStruct("newNestedField").getInt32("numberField"),
            actual.getStruct("newNestedField").getInt32("numberField")
        )
        assertEquals(
            expected.getStruct("newNestedField").getString("stringField"),
            actual.getStruct("newNestedField").getString("stringField")
        )
        assertEquals(expected.getArray<String>("newArray"), actual.getArray<String>("newArray"))
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
    fun testNestedArrayAcceptsEmptyValueOnTransformation() {
        // given
        val givenJsltTransformation = """
            {
                "nestedObject": .someObject 
            }
        """.trimIndent()
        configureNonTransformJslt(xformValue, givenJsltTransformation)

        val givenNestedSchema = SchemaBuilder
            .struct()
            .field("emptyArrayField", SchemaBuilder.array(Schema.STRING_SCHEMA))
        val givenInputSchema = SchemaBuilder
            .struct()
            .field("someObject", givenNestedSchema)

        val givenInputData = Struct(givenInputSchema)
            .put("someObject",
                Struct(givenNestedSchema).put(
                    "emptyArrayField", emptyList<String>()
                )
            )

        // when
        val actual: Struct = xformValue.apply(SourceRecord(null, null, "someTopic", 0,
            givenInputSchema, givenInputData)).value() as Struct

        // then
        assertNotNull(actual)
        assertNotNull(actual.getStruct("nestedObject"))
        assertEquals(emptyList<String>(),  actual.getStruct("nestedObject").getArray<String>("emptyArrayField"))
    }

    @Test
    fun testSchemalessConfigOptionToWriteOutputEvenIfNoSchemaCanBeInferred() {
        // given
        val givenJsltTransformation = """
            {
                "nestedObject": .
            }
        """.trimIndent()
        configureNonTransformJslt(xformValue, givenJsltTransformation, true)

        val legacyAttributeSchema = SchemaBuilder
            .struct()
            .field("code", Schema.STRING_SCHEMA)
            .field("values", SchemaBuilder.array(Schema.STRING_SCHEMA))
        val givenInputSchema = SchemaBuilder
            .struct()
            .field("legacyAttributes", SchemaBuilder.array(legacyAttributeSchema))

        val givenInputData = Struct(givenInputSchema)
            .put(
                "legacyAttributes",
                listOf<Struct>(
                    Struct(legacyAttributeSchema)
                        .put("code", "CONTACT/URL")
                        .put("values", emptyList<String>()),
                    Struct(legacyAttributeSchema)
                        .put("code", "VAN_MODEL/MODEL")
                        .put("values", listOf("Sonstige"))
                )
            )

        // when
        val actual: String = xformValue.apply(SourceRecord(null, null, "someTopic", 0,
            givenInputSchema, givenInputData)).value() as String

        // then
        assertEquals("{\"nestedObject\":{\"legacyAttributes\":[{\"code\":\"CONTACT/URL\",\"values\":[]},{\"code\":\"VAN_MODEL/MODEL\",\"values\":[\"Sonstige\"]}]}}", actual)
    }
}
