/*
 * Licensed to Facet Data, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Facet Data, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.frdy.codec

import com.fasterxml.jackson.core.{JsonFactory, JsonToken}
import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.scala.Predef._
import scala.collection.JavaConverters._

/**
 * Decodes smushed-together JSON objects, newline-delimited JSON objects, or a single array of JSON objects.
 */
class PlainJsonDecoder[A: ClassManifest](objectMapper: ObjectMapper) extends FrdyDecoder[A]
{
  require(
    objectMapper.getFactory.isInstanceOf[JsonFactory],
    "Expected JsonFactory, got: %s" format objectMapper.getFactory.getClass.getName
  )

  private val clazz = classManifest[A].erasure.asInstanceOf[Class[A]]

  private def isWhitespace(b: Byte) = {
    b == ' ' || b == '\n' || b == '\r' || b == '\t'
  }

  override def detect(bytes: Array[Byte], offset: Int, length: Int) = {
    var i = offset
    val limit = offset + length
    while (i < limit && isWhitespace(bytes(i))) {
      i += 1
    }
    i == limit || (i < limit && (bytes(i) == '{' || bytes(i) == '['))
  }

  override def decode(bytes: Array[Byte], offset: Int, length: Int) = {
    val jp = objectMapper.getFactory.createParser(bytes, offset, length)
    jp.nextToken()
    if (jp.getCurrentToken == JsonToken.START_ARRAY) {
      jp.nextToken()
    }
    objectMapper.readValues(jp, clazz).withFinally(_.close()) {
      values =>
        FrdyResult(values.asScala.toList, Map.empty)
    }
  }
}
