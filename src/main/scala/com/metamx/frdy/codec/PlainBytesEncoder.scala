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
import com.metamx.common.scala.untyped.Dict
import java.io.ByteArrayOutputStream

/**
 * Wraps arbitrary byte array into FRDY container with json header.
 * Note: despite FrdyEncoder interface accept sequence of messages to encode
 * PlainBytesEncoder supports only a single message encoding.
 *
 * Note: encoded message has no checksum.
 *
 * The data section (see FrdyContainer) has the format:
 * 1) 4 byte header length
 * 2) header bytes
 * 3) payload bytes
 */
class PlainBytesEncoder(headerObjectMapper: ObjectMapper) extends FrdyEncoder[Array[Byte]]
{
  val version: Byte = 4

  override def transcode(data: Array[Byte], offset: Int, length: Int, header: Dict): Array[Byte] =
    encode(Seq(data.slice(offset, offset + length)), header)

  override def encode(as: Seq[Array[Byte]], header: Dict): Array[Byte] = {
    require(as.size == 1, "Encoding only single a message is supported by PlainBytesEncoder")
    encode(as.head, header)
  }

  def encode(message: Array[Byte], header: Dict = Map.empty): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val jgHeader = headerObjectMapper.getFactory.createGenerator(baos)
    jgHeader.writeObject(header)
    jgHeader.close()
    val headerSize = baos.size()

    val dataLength = FrdyContainer.IntSize /* header size */ +
      headerSize +
      message.size

    val containerBuff = FrdyContainer.prepareBlock(version, dataLength, withChecksum = false)
    containerBuff.putInt(headerSize)
    containerBuff.put(baos.toByteArray)
    containerBuff.put(message)

    assert(containerBuff.position() == containerBuff.limit())
    containerBuff.array()
  }

  override def supportsHeaders: Boolean = true

}
