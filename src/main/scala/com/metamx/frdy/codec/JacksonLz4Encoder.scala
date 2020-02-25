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
import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.scala.untyped.Dict
import com.metamx.frdy.codec.FrdyContainer.IntSize
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import net.jpountz.lz4.LZ4Compressor

/**
 * Encodes Jackson-LZ4 FRDY blocks. The data section (see FrdyContainer) has the format:
 *
 * 1) 4 byte decompressed header length
 * 2) 4 byte decompressed payload length
 * 3) LZ4-compressed concatenated header and payload, encoded with outputMapper
 */
abstract class JacksonLz4Encoder[A](
  jsonMapper: ObjectMapper,
  outputMapper: ObjectMapper,
  compressor: LZ4Compressor
) extends FrdyEncoder[A]
{
  protected val version: Byte

  require(
    jsonMapper.getFactory.isInstanceOf[JsonFactory],
    "Expected JsonMapper, got: %s" format jsonMapper.getFactory.getClass.getName
  )

  protected def createBlock(srcData: Array[Byte], headerSize: Int, payloadSize: Int): Array[Byte] = {
    assert(headerSize + payloadSize == srcData.size)
    val blockData = Array.ofDim[Byte](IntSize + IntSize + compressor.maxCompressedLength(srcData.size))
    ByteBuffer.wrap(blockData).putInt(headerSize).putInt(payloadSize)
    val compressedSize = compressor.compress(srcData, 0, srcData.size, blockData, IntSize + IntSize)
    FrdyContainer.createBlock(version, blockData, 0, IntSize + IntSize + compressedSize)
  }

  override def transcode(json: Array[Byte], offset: Int, length: Int, header: Dict): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val jgHeader = outputMapper.getFactory.createGenerator(baos)
    jgHeader.writeObject(header)
    jgHeader.close()
    val headerSize = baos.size()

    val jpData = jsonMapper.getFactory.createParser(json, offset, length)
    val jgData = outputMapper.getFactory.createGenerator(baos)
    while (jpData.nextToken != null) {
      jgData.copyCurrentEvent(jpData)
    }
    jpData.close()
    jgData.close()
    createBlock(baos.toByteArray, headerSize, baos.size() - headerSize)
  }

  override def encode(as: Seq[A], header: Dict) = {
    // Generate uncompressed payload.
    val baos = new ByteArrayOutputStream
    val jgHeader = outputMapper.getFactory.createGenerator(baos)
    jgHeader.writeObject(header)
    jgHeader.close()
    val headerSize = baos.size()

    val jgData = outputMapper.getFactory.createGenerator(baos)
    as foreach jgData.writeObject
    jgData.close()
    createBlock(baos.toByteArray, headerSize, baos.size() - headerSize)
  }

  override def supportsHeaders = true
}
