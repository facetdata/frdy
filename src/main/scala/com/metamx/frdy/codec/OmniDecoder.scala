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

import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.metamx.common.scala.Jackson
import net.jpountz.lz4.LZ4Factory

class OmniDecoder[A](decoders: FrdyDecoder[A]*) extends FrdyDecoder[A]
{
  override def detect(bytes: Array[Byte], offset: Int, length: Int) = {
    decoders.exists(_.detect(bytes, offset, length))
  }

  override def decode(bytes: Array[Byte], offset: Int, length: Int) = {
    val decoder = decoders.find(_.detect(bytes, offset, length)) getOrElse {
      throw new IllegalStateException("Can't decode these bytes!")
    }
    decoder.decode(bytes, offset, length)
  }
}

object OmniDecoder
{
  def default[A: ClassManifest](modules: Module*): FrdyDecoder[A] = {
    val smileMapper = Jackson.newObjectMapper(new SmileFactory)
    val jsonMapper = Jackson.newObjectMapper()
    val lz4Decompressor = LZ4Factory.fastestJavaInstance().decompressor()
    smileMapper.registerModules(modules: _*)
    jsonMapper.registerModules(modules: _*)
    new OmniDecoder[A](
      new SmileLz4Decoder[A](smileMapper, lz4Decompressor),
      new JsonLz4Decoder[A](jsonMapper, lz4Decompressor),
      new PlainJsonDecoder[A](jsonMapper)
    )
  }
}
