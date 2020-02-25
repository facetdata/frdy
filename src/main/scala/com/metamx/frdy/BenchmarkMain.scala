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

package com.metamx.frdy

import com.fasterxml.jackson.dataformat.smile.{SmileFactory, SmileGenerator}
import com.google.common.io.Files
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.gz.{gunzip, gzip}
import com.metamx.common.scala.untyped.Dict
import com.metamx.frdy.codec.{JsonLz4Encoder, OmniDecoder, SmileLz4Encoder}
import java.io.File
import java.nio.ByteBuffer
import net.jpountz.lz4.LZ4Factory
import org.xerial.snappy.Snappy

object BenchmarkMain
{
  def main(args: Array[String]) {
    val file = new File(args(0))
    val bytes = Files.toByteArray(file)
    val lz4FastCompressor = LZ4Factory.fastestJavaInstance().fastCompressor()
    val lz4HighCompressor = LZ4Factory.fastestJavaInstance().highCompressor()
    val lz4Decompressor = LZ4Factory.fastestJavaInstance().decompressor()
    val decoder = OmniDecoder.default[Dict]()
    val objects = decoder.decode(bytes).objects
    val smileLz4EncoderFastCompression = new SmileLz4Encoder[Dict](
      Jackson.newObjectMapper(),
      Jackson.newObjectMapper(
        new SmileFactory() withEffect {
          sf =>
            sf.enable(SmileGenerator.Feature.CHECK_SHARED_NAMES)
            sf.disable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES)
        }
      ),
      lz4FastCompressor
    )
    val smileLz4EncoderFastCompressionSharedStrings = new SmileLz4Encoder[Dict](
      Jackson.newObjectMapper(),
      Jackson.newObjectMapper(
        new SmileFactory() withEffect {
          sf =>
            sf.enable(SmileGenerator.Feature.CHECK_SHARED_NAMES)
            sf.enable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES)
        }
      ),
      lz4FastCompressor
    )
    val smileLz4EncoderHighCompression = new SmileLz4Encoder[Dict](
      Jackson.newObjectMapper(),
      Jackson.newObjectMapper(
        new SmileFactory() withEffect {
          sf =>
            sf.enable(SmileGenerator.Feature.CHECK_SHARED_NAMES)
            sf.disable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES)
        }
      ),
      lz4HighCompressor
    )
    val jsonLz4EncoderFastCompression = new JsonLz4Encoder[Dict](
      Jackson.newObjectMapper(),
      lz4FastCompressor
    )
    val jsonLz4EncoderHighCompression = new JsonLz4Encoder[Dict](
      Jackson.newObjectMapper(),
      lz4HighCompressor
    )
    measure("json", 200)(
      () => bytes,
      (encoded: Array[Byte]) => decoder.decode(bytes).objects.size
    )
    measure("json-gzip", 200)(
      () => gzip(bytes),
      (encoded: Array[Byte]) => decoder.decode(gunzip(encoded)).objects.size
    )
    measure("json-snappy", 200)(
      () => Snappy.compress(bytes),
      (encoded: Array[Byte]) => decoder.decode(Snappy.uncompress(encoded)).objects.size
    )
    measure("json-lz4-fast", 200)(
      () => {
        val compressed = Array.ofDim[Byte](lz4FastCompressor.maxCompressedLength(bytes.size))
        val sz = lz4FastCompressor.compress(bytes, 0, bytes.length, compressed, 0)
        ByteBuffer.allocate(sz).put(compressed, 0, sz).array()
      },
      (encoded: Array[Byte]) => {
        val decompressed = Array.ofDim[Byte](bytes.size)
        lz4Decompressor.decompress(encoded, 0, decompressed, 0, bytes.size)
        decoder.decode(decompressed).objects.size
      }
    )
    measure("json-lz4-high", 200)(
      () => {
        val compressed = Array.ofDim[Byte](lz4FastCompressor.maxCompressedLength(bytes.size))
        val sz = lz4HighCompressor.compress(bytes, 0, bytes.length, compressed, 0)
        ByteBuffer.allocate(sz).put(compressed, 0, sz).array()
      },
      (encoded: Array[Byte]) => {
        val decompressed = Array.ofDim[Byte](bytes.size)
        lz4Decompressor.decompress(encoded, 0, decompressed, 0, bytes.size)
        decoder.decode(decompressed).objects.size
      }
    )
    measure("frdy-json-lz4-enc-fast", 200)(
      () => jsonLz4EncoderFastCompression.encode(objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-json-lz4-enc-high", 200)(
      () => jsonLz4EncoderHighCompression.encode(objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-json-lz4-trans-fast", 200)(
      () => jsonLz4EncoderFastCompression.transcode(bytes),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-json-lz4-trans-high", 200)(
      () => jsonLz4EncoderHighCompression.transcode(bytes),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-json-lz4-decenc-fast", 200)(
      () => jsonLz4EncoderFastCompression.encode(decoder.decode(bytes).objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-json-lz4-decenc-high", 200)(
      () => jsonLz4EncoderHighCompression.encode(decoder.decode(bytes).objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-enc-fast", 200)(
      () => smileLz4EncoderFastCompression.encode(objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-enc-fast-shared", 200)(
      () => smileLz4EncoderFastCompressionSharedStrings.encode(objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-enc-high", 200)(
      () => smileLz4EncoderHighCompression.encode(objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-trans-fast", 200)(
      () => smileLz4EncoderFastCompression.transcode(bytes),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-trans-fast-shared", 200)(
      () => smileLz4EncoderFastCompressionSharedStrings.transcode(bytes),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-trans-high", 200)(
      () => smileLz4EncoderHighCompression.transcode(bytes),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-decenc-fast", 200)(
      () => smileLz4EncoderFastCompression.encode(decoder.decode(bytes).objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-decenc-fast-shared", 200)(
      () => smileLz4EncoderFastCompressionSharedStrings.encode(decoder.decode(bytes).objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
    measure("frdy-smile-lz4-decenc-high", 200)(
      () => smileLz4EncoderHighCompression.encode(decoder.decode(bytes).objects),
      (encoded: Array[Byte]) => decoder.decode(encoded).objects.size
    )
  }

  def time[A](reps: Int, f: () => A): (Long /*µs*/, A) = {
    (for (run <- 1 to 2) yield {
      System.gc()
      val start = System.nanoTime()
      var ret: Option[A] = None
      for (i <- 1 to reps) {
        ret = Some(f())
      }
      ((System.nanoTime() - start) / 1000 / reps, ret.get)
    }).last
  }

  def measure(name: String, reps: Int)(encode: () => Array[Byte], decode: Array[Byte] => Int) {
    val (encTime, encBytes) = time(reps, encode)
    val (decTime, decCount) = time(reps, () => decode(encBytes))
    println(Seq(name, encTime, encBytes.size, decTime, decCount).mkString("\t"))
  }
}
