package at.willhaben.kafka.connect.transforms.jslt

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.schibsted.spt.data.jslt.Expression
import com.schibsted.spt.data.jslt.Parser
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.*
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.util.Collections.singletonMap


abstract class JsltTransform<R : ConnectRecord<R>?>() : Transformation<R> {
    companion object {
        val OVERVIEW_DOC =
            ("Flatten a nested data structure, generating names for each field by concatenating the field names at each "
                    + "level with a configurable delimiter character. Applies to Struct when schema present, or a Map "
                    + "in the case of schemaless data. Array fields and their contents are not modified. The default delimiter is '.'."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key::class.java.name + "</code>) "
                    + "or value (<code>" + Value::class.java.name + "</code>).")
        private val JSLT_CONFIG = "jslt"
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
    private var schemaUpdateCache: Cache<Schema?, Schema?>? = null

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)
        jslt = config.getString(JSLT_CONFIG)
        jsltExpression = Parser.compileString(jslt)
        schemaUpdateCache = SynchronizedCache(LRUCache(16))
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

    override fun close() {}

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

    protected abstract fun operatingSchema(record: R?): Schema?

    protected abstract fun operatingValue(record: R?): Any?

    protected abstract fun newRecord(record: R?, updatedSchema: Schema?, updatedValue: Any?): R

    private fun operatingTopic(record: R?): String? = record?.topic()

    protected abstract fun getJsonConverterConfig(record: R?): Pair<Map<String, Any>, Boolean>


    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)
        val inputValueJsonNode = objectMapper.valueToTree<JsonNode>(value)
        val outputValueJsonNode = jsltExpression.apply(inputValueJsonNode)
        return newRecord(record, null, outputValueJsonNode)
    }


    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStructOrNull(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record)
        val topic = operatingTopic(record)

        return if (value == null) {
            newRecord(record, schema, null)
        } else {
            val valueAsJsonBytes = jsonConverter.fromConnectData(topic, schema, value)
            val inputValueJsonNode = objectMapper.readTree(valueAsJsonBytes)
            val outputValueJsonNode = jsltExpression.apply(inputValueJsonNode)

            var outputSchema = schemaUpdateCache!![schema]
            if (outputSchema == null) {
                outputSchema = jsonConverter.asConnectSchema(outputValueJsonNode)
                schemaUpdateCache!!.put(schema, outputSchema)
            }
            val outputValue =objectMapper.convertValue(outputValueJsonNode, object: TypeReference<Map<String, Any>>() {})
            newRecord(record, outputSchema, outputValue)
        }
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