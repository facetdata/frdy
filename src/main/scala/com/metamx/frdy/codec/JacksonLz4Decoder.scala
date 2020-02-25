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

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.untyped.Dict
import com.metamx.frdy.codec.FrdyContainer._
import java.nio.ByteBuffer
import net.jpountz.lz4.LZ4Decompressor
import scala.collection.JavaConverters._

/**
 * See JacksonLz4Encoder.
 */
abstract class JacksonLz4Decoder[A: ClassManifest](
  objectMapper: ObjectMapper,
  decompressor: LZ4Decompressor
) extends FrdyDecoder[A]
{
  private val clazz = classManifest[A].erasure.asInstanceOf[Class[A]]

  protected val version: Byte

  override def detect(bytes: Array[Byte], offset: Int, length: Int) = {
    length > ContainerOverhead +
      IntSize /* decompressed header size */ +
      IntSize /* decompressed payload size */ &&
      detectMagicAndVersion(bytes, offset, version)
  }

  override def decode(bytes: Array[Byte], offset: Int, length: Int) = {
    if (!detect(bytes, offset, length)) {
      throw new IllegalStateException("This buffer isn't very magical.")
    }

    verifyChecksum(bytes, offset, length)

    // Decompress payload.
    val (dataOffset, dataSize) = extractData(offset, length)
    val compressedOffset = dataOffset + IntSize + IntSize
    val compressedSize = dataSize - IntSize - IntSize
    val headerSize = ByteBuffer.wrap(bytes, dataOffset, IntSize).getInt
    val payloadSize = ByteBuffer.wrap(bytes, dataOffset + IntSize, IntSize).getInt
    val decompressedData = Array.ofDim[Byte](headerSize + payloadSize)
    val compressedBytesRead = decompressor.decompress(
      bytes, compressedOffset, decompressedData, 0, headerSize + payloadSize
    )
    if (compressedBytesRead != compressedSize) {
      throw new IllegalStateException(
        "Oops, expected to read %,d bytes, but actually read %,d bytes." format
          (compressedSize, compressedBytesRead)
      )
    }

    // Decode header.
    val header = objectMapper.readValue(decompressedData, 0, headerSize, classOf[Dict])

    // Decode objects.
    val jp = objectMapper.getFactory.createParser(decompressedData, headerSize, payloadSize)
    jp.nextToken()
    if (jp.getCurrentToken == JsonToken.START_ARRAY) {
      jp.nextToken()
    }
    objectMapper.readValues(jp, clazz).withFinally(_.close()) {
      values =>
        FrdyResult(values.asScala.toList, header)
    }
  }
}
