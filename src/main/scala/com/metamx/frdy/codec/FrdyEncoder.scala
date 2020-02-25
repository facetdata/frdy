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

import com.metamx.common.scala.untyped.Dict

/**
 * FrdyEncoders encode blocks of objects, and optionally some extra key/value pairs (the "header") into some target
 * format. They are not stream encoders, and need to be wrapped in some other apparatus if they will be used to encode
 * large amounts of data.
 */
trait FrdyEncoder[A]
{
  /**
   * Encode data from a JSON buffer. This allows encoders to provide an efficient way to translate JSON directly to
   * the target format. Encoders are free to assume the buffer is valid JSON, even if this leads to invalid encoded
   * data. The encoder will not modify or retain a reference to the byte array, so you can reuse it if you want.
   */
  def transcode(json: Array[Byte], offset: Int, length: Int, header: Dict): Array[Byte]

  /**
   * Encode objects directly.
   */
  def encode(as: Seq[A], header: Dict): Array[Byte]

  /**
   * Does this encoder support writing headers? If false, you should not pass headers to the encode or
   * transcode methods.
   */
  def supportsHeaders: Boolean

  /**
   * Equivalent to transcode(json, 0, json.size, Map.empty).
   */
  final def transcode(json: Array[Byte]): Array[Byte] = transcode(json, 0, json.size, Map.empty)

  /**
   * Equivalent to transcode(json, 0, json.size, header).
   */
  final def transcode(json: Array[Byte], header: Dict): Array[Byte] = transcode(json, 0, json.size, header)

  /**
   * Equivalent to transcode(json, offset, length, Map.empty).
   */
  final def transcode(json: Array[Byte], offset: Int, length: Int): Array[Byte] = transcode(json, offset, length, Map.empty)

  /**
   * Equivalent to encode(as, Map.empty).
   */
  final def encode(as: Seq[A]): Array[Byte] = encode(as, Map.empty)
}
