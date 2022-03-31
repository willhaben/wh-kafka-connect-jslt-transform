# Kafka Connect JSLT Single Message Transform

This is an implementation of the [Kafka Connect SMT](https://docs.confluent.io/platform/current/connect/transforms/overview.html) interface 
to offer transformation capabilities using the Schibsted [JSLT library](https://github.com/schibsted/jslt).

## Build

The library uses Gradle to build the JAR.

1. Install latest Java SDK.
2. Checkout the Git repository and change to its root folder.
3. Execute `./gradlew build`

The JAR can then be found in the `build/libs/` subfolder.

## Install

After the JAR was build as described above, copy it to your Kafka Connect instance into one of the directories listed in
the `plugin.path` property in the connect worker configuration file.
> Make sure to do this on all Kafka Connect worker nodes!

See the [Confluent Kafka Connect Plugins Userguide](https://docs.confluent.io/home/connect/self-managed/userguide.html#installing-kconnect-plugins) for more details.

## Usage

### Connector Configuration

The transformer expects the `jslt` attribute in the connector config JSON. 
In the following example the dot means it will not perform any transformation but this string can be any valid JSLT expression.
> Note that Json does not support multiline strings. So linebreaks and quotes must be escaped with a backslash (e.g. `"` -> `\"`) 

```json
{
  "name": ...,
  "config": {
    ...,
    "transforms": "jsltTransform",
    "transforms.jsltTransform.type": "at.willhaben.kafka.connect.transforms.jslt.JsltTransform$Value",
    "transforms.jsltTransform.jslt": "."
  }
}
```

> Note that the transformer only supports some kind of structured input. So make sure that there is a [converter](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/) class (e.g. [AvroConverter](https://www.confluent.io/hub/confluentinc/kafka-connect-avro-converter), [JsonConverter](https://www.confluent.io/hub/confluentinc/kafka-connect-json-schema-converter)) like `"value.converter": "io.confluent.connect.avro.AvroConverter"` that provides the data in a structured format.

The following links are helpful to learn more about JSLT:

* [JSLT Tutorial](https://github.com/schibsted/jslt/blob/master/tutorial.md)
  and [Examples](https://github.com/schibsted/jslt/blob/master/examples/README.md): examples of the JSLT language
  features
* [JSLT Function Reference](https://github.com/schibsted/jslt/blob/master/functions.md): Documentation of the included
  functions
* [JSLT Demo Playground](https://www.garshol.priv.no/jslt-demo): comes in handy to try out the logic

### Example JSLT

The following JSLT takes a nested structure as input and

* adds a new constant field `producer_team`
* maps the values of the field `customer_status` from a numeric representation to a code, using custom logic
* pseudonymizes the `customer_id` field
* adds a list of objects field `locations` based on the separate invoice/delivery address fields of the input
* extracts and flattens the two attributes `customer_type` and `customer_class`

**Input Record**

```json
{
  "cusomer_id": 123456,
  "customer_status": 2,
  "invoice_address_street": "McDuck Manor",
  "invoice_address_zip_code": "1312",
  "invoice_address_city": "Duckburg",
  "delivery_address_street": "Webfoot Walk",
  "delivery_address_zip_code": "1313",
  "delivery_address_city": "Duckburg",
  "attributes": {
    "customer_type": "C2C",
    "customer_class": "A",
    "last_order": "2022-03-10T18:25:43.511Z"
  }
}
```

**JSLT**

```
{
  def map_status(status)
  if ($status == 0)
    "ACTIVE"
  else if ($status == 1)
    "INACTIVE"
  else
    "UNDEFINED"

  "customer_id": sha256-hex(.customer_id),
  "customer_status_code": map_status(.customer_status),
  "locations": [
    {
      "zip_code": .invoice_address_zip_code,
      "city": .invoice_address_city
    },
    {
      "zip_code": .delivery_address_zip_code,
      "city": .delivery_address_city
    }
  ],
  "customer_type": .attributes.customer_type,
  "customer_class": .attributes.customer_class,
  "producer_team": "us.california.burbank.disney"
}
```

**Output Record**

```json
{
  "customer_id": "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92",
  "customer_status_code": "INACTIVE",
  "locations": [
    {
      "zip_code": "1312",
      "city": "Duckburg"
    },
    {
      "zip_code": "1313",
      "city": "Duckburg"
    }
  ],
  "customer_type": "C2C",
  "customer_class": "A",
  "producer_team": "us.california.burbank.disney"
}
```

### Additional Notes

The JSLT library works with
the [JsonNode](https://fasterxml.github.io/jackson-databind/javadoc/2.8/com/fasterxml/jackson/databind/JsonNode.html)
class to perform the transformation.

This class uses quite generic data types and the conversion to/from JsonNode could lead to some changes in the returned
datatype. This might especially an issue with floating point values!
