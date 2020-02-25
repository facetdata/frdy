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

/**
 * FrdyDecoders decode blocks of objects that have been encoded by FrdyEncoders.
 */
trait FrdyDecoder[A]
{
  /**
   * Examine a block and determine if this decoder will be able to decode it. This should be far cheaper than the
   * decode operation.
   */
  def detect(bytes: Array[Byte], offset: Int, length: Int): Boolean

  /**
   * Decode a full block. Returns the objects and the header.
   */
  def decode(bytes: Array[Byte], offset: Int, length: Int): FrdyResult[A]

  /**
   * Equivalent to detect(bytes, 0, bytes.size).
   */
  final def detect(bytes: Array[Byte]): Boolean = detect(bytes, 0, bytes.size)

  /**
   * Equivalent to decode(bytes, 0, bytes.size).
   */
  final def decode(bytes: Array[Byte]): FrdyResult[A] = decode(bytes, 0, bytes.size)
}
