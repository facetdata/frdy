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
import net.jpountz.lz4.LZ4Compressor

class JsonLz4Encoder[A](
  jsonMapper: ObjectMapper,
  compressor: LZ4Compressor
) extends JacksonLz4Encoder[A](
  jsonMapper,
  jsonMapper,
  compressor
)
{
  override protected val version: Byte = 1

  override def transcode(json: Array[Byte], offset: Int, length: Int, header: Dict) = {
    val baos = new ByteArrayOutputStream
    val jgHeader = jsonMapper.getFactory.createGenerator(baos)
    jgHeader.writeObject(header)
    jgHeader.close()
    val headerSize = baos.size()

    baos.write(json, offset, length)
    createBlock(baos.toByteArray, headerSize, baos.size() - headerSize)
  }
}
