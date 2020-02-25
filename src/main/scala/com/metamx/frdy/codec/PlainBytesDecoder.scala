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

import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.scala.untyped._
import com.metamx.frdy.codec.FrdyContainer._
import java.nio.ByteBuffer

/**
 * See PlainBytesEncoder.
 */
class PlainBytesDecoder(headerObjectMapper: ObjectMapper) extends FrdyDecoder[Array[Byte]]
{
  val version: Byte = 4
  val minSize = ContainerOverhead - ChecksumSize + IntSize /* int for header size */

  override def detect(bytes: Array[Byte], offset: Int, length: Int): Boolean = {
    length >= minSize && detectMagicAndVersion(bytes, offset, version)
  }

  override def decode(bytes: Array[Byte], offset: Int, length: Int): FrdyResult[Array[Byte]] = {
    if (!detect(bytes, offset, length)) {
      throw new IllegalStateException("This buffer isn't encoded with PlainBytesEncoder")
    }

    val (dataOffset, dataSize) = extractData(offset, length, withChecksum = false)
    val data = ByteBuffer.wrap(bytes, dataOffset, dataSize)

    val headerSize = data.getInt
    if (data.remaining() <  headerSize) {
      throw new IllegalStateException(
        "Oops, expected to read %,d bytes, but actually need to read %,d bytes or more." format
          (dataSize, IntSize + headerSize)
      )
    }

    val header = headerObjectMapper.readValue(data.array(), data.position(), headerSize, classOf[Dict])
    data.position(data.position() + headerSize)
    val item = new Array[Byte](data.remaining())
    data.get(item)
    FrdyResult(Seq(item), header)
  }

  def decodePayloadOnly(bytes: Array[Byte]): Array[Byte] =
    decodePayloadOnly(bytes, 0, bytes.length)

  def decodePayloadOnly(bytes: Array[Byte], offset: Int , length: Int): Array[Byte] = {
    if (!detect(bytes, offset, length)) {
      throw new IllegalStateException("This buffer isn't encoded with PlainBytesEncoder")
    }

    val (dataOffset, dataSize) = extractData(offset, length, withChecksum = false)
    val data = ByteBuffer.wrap(bytes, dataOffset, dataSize)

    val headerSize = data.getInt
    if (data.remaining() < headerSize) {
      throw new IllegalStateException(
        "Oops, expected to read %,d bytes, but actually need to read %,d bytes or more." format
          (dataSize, IntSize + headerSize)
      )
    }
    data.position(data.position() + headerSize)
    val itemSize = dataSize - IntSize - headerSize
    val item = new Array[Byte](itemSize)
    data.get(item)
    item
  }

}
