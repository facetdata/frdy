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

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.io.SerializedString
import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.scala.untyped.Dict
import java.io.ByteArrayOutputStream

/**
 * Encodes JSON objects. This encoder does not allow headers, since it simply emits plain text with no container.
 */
class PlainJsonEncoder[A](jsonMapper: ObjectMapper) extends FrdyEncoder[A]
{
  require(
    jsonMapper.getFactory.isInstanceOf[JsonFactory],
    "Expected JsonFactory, got: %s" format jsonMapper.getFactory.getClass.getName
  )

  override def transcode(json: Array[Byte], offset: Int, length: Int, header: Dict) = {
    require(header.isEmpty, "headers not supported")
    val buf = new Array[Byte](length)
    System.arraycopy(json, offset, buf, 0, length)
    buf
  }

  override def encode(as: Seq[A], header: Dict) = {
    require(header.isEmpty, "headers not supported")
    val baos = new ByteArrayOutputStream
    val jg = jsonMapper.getFactory.createGenerator(baos)
    jg.setRootValueSeparator(new SerializedString("\n"))
    for (a <- as) {
      jg.writeObject(a)
    }
    jg.close()
    if (as.nonEmpty) {
      baos.write('\n')
    }
    baos.toByteArray
  }

  override def supportsHeaders = false
}
