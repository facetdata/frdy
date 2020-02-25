package com.metamx.frdy.test

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}

class FunkyModule extends SimpleModule
{
  addSerializer(
    classOf[FunkyClass],
    new JsonSerializer[FunkyClass]
    {
      override def serialize(value: FunkyClass, jg: JsonGenerator, provider: SerializerProvider) {
        jg.writeStartObject()
        jg.writeStringField("funkyFoo", value.foo)
        jg.writeEndObject()
      }
    }
  )

  addDeserializer(
    classOf[FunkyClass],
    new JsonDeserializer[FunkyClass]
    {
      override def deserialize(jp: JsonParser, context: DeserializationContext) = {
        if (jp.getCurrentToken != JsonToken.START_OBJECT) {
          throw context.mappingException("expected START_OBJECT")
        }
        jp.nextToken()
        if (jp.getCurrentToken != JsonToken.FIELD_NAME || jp.getText != "funkyFoo") {
          throw context.mappingException("expected field 'funkyFoo'")
        }
        jp.nextToken()
        if (jp.getCurrentToken != JsonToken.VALUE_STRING) {
          throw context.mappingException("expected VALUE_STRING")
        }
        val foo = jp.getText
        jp.nextToken()
        if (jp.getCurrentToken != JsonToken.END_OBJECT) {
          throw context.mappingException("expected END_OBJECT")
        }
        new FunkyClass(foo)
      }
    }
  )
}
