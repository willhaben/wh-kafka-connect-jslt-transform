package at.willhaben.kafka.connect.transforms.jslt

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.schibsted.spt.data.jslt.Expression
import com.schibsted.spt.data.jslt.Parser
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.util.Collections.singletonMap


@Suppress("TooManyFunctions")
abstract class JsltTransform<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        val OVERVIEW_DOC =
            ("Transform a structured record using the JSLT library. See https://github.com/schibsted/jslt." +
                    "NOTE: JSLT is working with JsonNode objects, " +
                    "which means that any field is converted and the resulting field " +
                    "might not be of the exact same type as the input field!" +
                    "" +
                    "<p/>Use the concrete transformation type designed for " +
                    "the record key (<code>" + Key::class.java.name + "</code>) " +
                    "or value (<code>" + Value::class.java.name + "</code>).")
        private const val JSLT_CONFIG = "jslt"
        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                JSLT_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.MEDIUM,
                "JSLT expression that returns the transformed object"
            )
        private const val PURPOSE = "jslt-transform"
    }

    private val jsonConverter: JsonConverter = JsonConverter()
    private val objectMapper = ObjectMapper()

    private lateinit var jslt: String
    private lateinit var jsltExpression: Expression

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)
        jslt = config.getString(JSLT_CONFIG)
        jsltExpression = Parser.compileString(jslt)
    }

    override fun apply(record: R): R {
        val (configMap, isKeyFlag) = getJsonConverterConfig(record)
        jsonConverter.configure(configMap, isKeyFlag)
        return when {
            operatingValue(record) == null -> {
                record
            }
            operatingSchema(record) == null -> {
                applySchemaless(record)
            }
            else -> {
                applyWithSchema(record)
            }
        }
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

    protected abstract fun operatingSchema(record: R?): Schema?

    protected abstract fun operatingValue(record: R?): Any?

    protected abstract fun newRecord(record: R?, updatedSchema: Schema?, updatedValue: Any?): R

    protected abstract fun getJsonConverterConfig(record: R?): Pair<Map<String, Any>, Boolean>


    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)
        val inputValueJsonNode = objectMapper.valueToTree<JsonNode>(value)
        val outputValue = jsltExpression.apply(inputValueJsonNode).toString()
        return newRecord(record, null, outputValue)
    }


    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record)
        val topic = record?.topic()

        val valueAsJsonBytes = jsonConverter.fromConnectData(topic, schema, value)
        val inputValueJsonNode = objectMapper.readTree(valueAsJsonBytes)
        val outputValue = convert(inputValueJsonNode)
        return newRecord(record, outputValue?.schema(), outputValue)
    }

    private fun convert(inputValueJsonNode: JsonNode): Struct? {
        val outputValueJsonNode = jsltExpression.apply(inputValueJsonNode)
        return if (outputValueJsonNode != null) {
            val outputSchema = schemaFromJsonObject(outputValueJsonNode)
            val outputValue = Struct(outputSchema)
            outputValueJsonNode.fields().forEach { (fieldName, fieldValue) ->
                jsonNodeToStruct(fieldValue, outputSchema.field(fieldName).schema(), outputValue, fieldName)
            }
            outputValue
        } else {
            null
        }
    }

    private fun jsonNodeToStruct(
        jsonNode: JsonNode,
        schema: Schema,
        struct: Struct? = null,
        fieldName: String? = null,
    ) {
        when (jsonNode.nodeType) {
            JsonNodeType.ARRAY -> {
                val array = convertJsonNodeToValue(jsonNode, schema)
                struct?.put(fieldName, array)
            }
            JsonNodeType.POJO, JsonNodeType.OBJECT -> {
                val subStruct = convertJsonNodeToValue(jsonNode, schema)
                struct?.put(fieldName, subStruct)
            }
            else -> {
                val value = convertJsonNodeToValue(jsonNode)
                struct?.put(fieldName, value)
            }
        }
    }

    private fun convertJsonNodeToValue(jsonNode: JsonNode, schema: Schema? = null): Any? {
        return when (jsonNode.nodeType!!) {
            JsonNodeType.ARRAY -> {
                if (jsonNode.elements().hasNext()) {
                    jsonNode.elements().asSequence().map { elem ->
                        convertJsonNodeToValue(elem, schema!!.valueSchema())
                    }.toList()
                } else {
                    null
                }
            }
            JsonNodeType.BINARY -> jsonNode.binaryValue()
            JsonNodeType.BOOLEAN -> jsonNode.booleanValue()
            JsonNodeType.MISSING -> null
            JsonNodeType.NULL -> null
            JsonNodeType.NUMBER -> getNumberValue(jsonNode)
            JsonNodeType.POJO, JsonNodeType.OBJECT -> {
                val subStruct = Struct(schema)
                jsonNode.fields().forEach { (key, field) ->
                    jsonNodeToStruct(field, schema!!.field(key).schema(), subStruct, key)
                }
                subStruct
            }
            JsonNodeType.STRING -> jsonNode.textValue()
        }
    }

    private fun getNumberValue(jsonNode: JsonNode): Any? {
        return when {
            jsonNode.isFloat -> jsonNode.floatValue()
            jsonNode.isDouble -> jsonNode.doubleValue()
            jsonNode.isBigDecimal -> jsonNode.decimalValue().toPlainString()
            jsonNode.isShort -> jsonNode.shortValue()
            jsonNode.isInt -> jsonNode.intValue()
            jsonNode.isLong -> jsonNode.longValue()
            jsonNode.isBigInteger -> jsonNode.bigIntegerValue()
            else -> if (jsonNode.isFloatingPointNumber) jsonNode.doubleValue() else jsonNode.asLong()
        }
    }

    private fun schemaFromJsonObject(jsonNode: JsonNode): Schema {
        val schemaBuilder = SchemaBuilder(Schema.Type.STRUCT)
        jsonNode.fields().forEach { field ->
            if (field.value.nodeType == JsonNodeType.ARRAY) {
                schemaBuilder.field(field.key, schemaFromJsonArray(field.value))
            } else if (field.value.nodeType == JsonNodeType.OBJECT || field.value.nodeType == JsonNodeType.POJO) {
                schemaBuilder.field(field.key, schemaFromJsonObject(field.value))
            } else {
                schemaBuilder.field(field.key, getPrimitiveType(field.value))
            }
        }
        return schemaBuilder.build()
    }

    private fun schemaFromJsonArray(jsonNode: JsonNode): Schema {
        return if (jsonNode.elements().hasNext()) {
            val element = jsonNode.elements().next()
            if (element.nodeType == JsonNodeType.OBJECT || element.nodeType == JsonNodeType.POJO) {
                SchemaBuilder.array(schemaFromJsonObject(element)).build()
            } else if (element.nodeType == JsonNodeType.ARRAY) {
                SchemaBuilder.array(schemaFromJsonArray(element))
            } else {
                SchemaBuilder.array(getPrimitiveType(element)).build()
            }
        } else {
            SchemaBuilder(Schema.Type.ARRAY).build()
        }
    }

    private fun getPrimitiveType(jsonNode: JsonNode): Schema {
        return when (jsonNode.nodeType) {
            JsonNodeType.BINARY -> Schema.OPTIONAL_BYTES_SCHEMA
            JsonNodeType.BOOLEAN -> Schema.OPTIONAL_BOOLEAN_SCHEMA
            JsonNodeType.STRING -> Schema.OPTIONAL_STRING_SCHEMA
            JsonNodeType.NULL -> Schema.OPTIONAL_STRING_SCHEMA
            JsonNodeType.NUMBER -> getNumberSchema(jsonNode)
            JsonNodeType.MISSING -> Schema.OPTIONAL_STRING_SCHEMA
            else -> throw DataException("The type ${jsonNode.nodeType} is not a primitive!")
        }
    }

    private fun getNumberSchema(jsonNode: JsonNode) = when {
        jsonNode.isShort -> Schema.OPTIONAL_INT16_SCHEMA
        jsonNode.isInt -> Schema.OPTIONAL_INT32_SCHEMA
        jsonNode.isLong -> Schema.OPTIONAL_INT64_SCHEMA
        jsonNode.isBigInteger -> Schema.OPTIONAL_STRING_SCHEMA
        jsonNode.isFloat -> Schema.OPTIONAL_FLOAT32_SCHEMA
        jsonNode.isDouble -> Schema.OPTIONAL_FLOAT64_SCHEMA
        jsonNode.isBigDecimal -> Schema.OPTIONAL_STRING_SCHEMA
        else -> throw DataException("Unsupported numerical type for $jsonNode")
    }


    class Key<R : ConnectRecord<R>?> : JsltTransform<R>() {
        override fun getJsonConverterConfig(record: R?): Pair<Map<String, Any>, Boolean> =
            Pair(singletonMap("schemas.enable", record?.keySchema() != null), true)

        override fun operatingSchema(record: R?): Schema? = record?.keySchema()

        override fun operatingValue(record: R?): Any? = record?.key()

        override fun newRecord(record: R?, updatedSchema: Schema?, updatedValue: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            updatedSchema,
            updatedValue,
            record.valueSchema(),
            record.value(),
            record.timestamp()
        )
    }

    class Value<R : ConnectRecord<R>?> : JsltTransform<R>() {
        override fun getJsonConverterConfig(record: R?): Pair<Map<String, Any>, Boolean> =
            Pair(singletonMap("schemas.enable", record?.keySchema() != null), false)

        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()

        override fun operatingValue(record: R?): Any? = record?.value()

        override fun newRecord(record: R?, updatedSchema: Schema?, updatedValue: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            updatedSchema,
            updatedValue,
            record.timestamp()
        )
    }
}
