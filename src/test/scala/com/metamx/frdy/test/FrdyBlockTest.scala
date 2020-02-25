package com.metamx.frdy.test

import com.github.nscala_time.time.Imports._
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.google.common.base.Charsets
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.untyped.Dict
import com.metamx.frdy.codec.JsonLz4Decoder
import com.metamx.frdy.codec.JsonLz4Encoder
import com.metamx.frdy.codec.OmniDecoder
import com.metamx.frdy.codec.PlainJsonDecoder
import com.metamx.frdy.codec.PlainJsonEncoder
import com.metamx.frdy.codec.SmileLz4Decoder
import com.metamx.frdy.codec.SmileLz4Encoder
import net.jpountz.lz4.LZ4Factory
import org.scalatest.FunSuite

class FrdyBlockTest extends FunSuite
{
  val dicts = Seq(Dict("hey" -> "what"), Dict("foo" -> Seq(Map("bar" -> "b\na\tz"))))

  val jsonArray      = """[{"hey":"what"},{"foo":[{"bar":"b\na\tz"}]}]""".getBytes(Charsets.UTF_8)
  val jsonNewlines   = ("""{"hey":"what"}""" + "\n" + """{"foo":[{"bar":"b\na\tz"}]}""" + "\n").getBytes(Charsets.UTF_8)
  val jsonSmushed    = """{"hey":"what"}{"foo":[{"bar":"b\na\tz"}]}""".getBytes(Charsets.UTF_8)
  val jsonWhitespace = (" \n \t \n " + """{"hey":"what"}{"foo":[{"bar":"b\na\tz"}]}""").getBytes(Charsets.UTF_8)
  val jsons          = Seq(
    "array" -> jsonArray,
    "newlines" -> jsonNewlines,
    "smushed" -> jsonSmushed,
    "whitespace" -> jsonWhitespace
  )
  val smileLz4Block  = Array[Byte](
    -16, -97, -104, -76, 2, 0, 0, 0, 6, 0, 0, 0, 35, 97, 58, 41, 10, 1, -6, -5, 6, 0, -16, 15, -126, 104, 101, 121, 67,
    119, 104, 97, 116, -5, -6, -126, 102, 111, 111, -8, -6, -126, 98, 97, 114, 68, 98, 10, 97, 9, 122, -5, -7, -5, -92,
    82, 45, -127
  )
  val emptyBuffers = Seq(
    "empty" -> Array[Byte](),
    "spaces" -> Array[Byte](' ', ' '),
    "newlines" -> Array[Byte]('\n', '\n'),
    "spaces-and-newlines" -> Array[Byte](' ', '\n', ' ', '\n')
  )
  val garbage = (0 until 100).flatMap(_ => "}LOL]".getBytes(Charsets.UTF_8)).toArray

  val smileLz4Encoder  = new SmileLz4Encoder[Dict](
    Jackson.newObjectMapper(),
    Jackson.newObjectMapper(new SmileFactory()),
    LZ4Factory.fastestJavaInstance().fastCompressor()
  )
  val smileLz4Decoder  = new SmileLz4Decoder[Dict](
    Jackson.newObjectMapper(new SmileFactory()),
    LZ4Factory.fastestJavaInstance().decompressor()
  )
  val jsonLz4Encoder   = new JsonLz4Encoder[Dict](
    Jackson.newObjectMapper(),
    LZ4Factory.fastestJavaInstance().fastCompressor()
  )
  val jsonLz4Decoder   = new JsonLz4Decoder[Dict](
    Jackson.newObjectMapper(),
    LZ4Factory.fastestJavaInstance().decompressor()
  )
  val plainJsonEncoder = new PlainJsonEncoder[Dict](Jackson.newObjectMapper())
  val plainJsonDecoder = new PlainJsonDecoder[Dict](Jackson.newObjectMapper())
  val omniDecoder      = new OmniDecoder[Dict](
    smileLz4Decoder,
    jsonLz4Decoder,
    plainJsonDecoder
  )
  val codecs           = Seq(
    ("smile-lz4", smileLz4Encoder, smileLz4Decoder),
    ("json-lz4", jsonLz4Encoder, jsonLz4Decoder),
    ("json-plain", plainJsonEncoder, plainJsonDecoder)
  )

  test("Plain-JSON decoding + encoding") {
    for ((label, json) <- jsons) {
      val roundTrip = plainJsonEncoder.encode(plainJsonDecoder.decode(json).objects)
      assert(roundTrip.deep === jsonNewlines.deep)
    }
  }

  test("Plain-JSON decoding through omni decoder") {
    for ((label, json) <- jsons) {
      val parsed = omniDecoder.decode(json)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === Map(), label)
    }
  }

  test("Round-trips") {
    for ((label, encoder, decoder) <- codecs) {
      val bytes = encoder.encode(dicts)
      val parsed = decoder.decode(bytes)
      val parsedOmni = omniDecoder.decode(bytes)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === Map(), label)
      assert(parsedOmni.objects === dicts, label)
      assert(parsedOmni.header === Map(), label)
    }
  }

  test("Round-trips from partial buffers") {
    for ((label, encoder, decoder) <- codecs) {
      val bytes = encoder.encode(dicts)
      val bytesWithGarbage = garbage ++ bytes ++ garbage
      val parsed = decoder.decode(bytesWithGarbage, garbage.size, bytes.size)
      val parsedOmni = omniDecoder.decode(bytesWithGarbage, garbage.size, bytes.size)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === Map(), label)
      assert(parsedOmni.objects === dicts, label)
      assert(parsedOmni.header === Map(), label)
    }
  }

  test("Round-trips with headers") {
    val header = Map("ts" -> new DateTime("2258").getMillis)

    // Plain JSON codec doesn't support headers, so skip it.
    for ((label, encoder, decoder) <- codecs if label != "json-plain") {
      val bytes = encoder.encode(dicts, header)
      val parsed = decoder.decode(bytes, 0, bytes.length)
      val parsedOmni = omniDecoder.decode(bytes)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === header, label)
      assert(parsedOmni.objects === dicts, label)
      assert(parsedOmni.header === header, label)
    }
  }

  test("Encoding with headers into plain JSON (should fail)") {
    val header = Map("ts" -> new DateTime("2258").getMillis)
    val e = intercept[IllegalArgumentException] {
      plainJsonEncoder.encode(dicts, header)
    }
    assert(e.getMessage === "requirement failed: headers not supported")
  }

  test("Transcoding") {
    for ((codecLabel, encoder, decoder) <- codecs; (jsonLabel, json) <- jsons) {
      val label = "codec[%s] json[%s]" format(codecLabel, jsonLabel)
      val bytes = encoder.transcode(json)
      val parsed = decoder.decode(bytes)
      val parsedOmni = omniDecoder.decode(bytes)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === Map(), label)
      assert(parsedOmni.objects === dicts, label)
      assert(parsedOmni.header === Map(), label)
    }
  }

  test("Transcoding from partial buffers") {
    for ((codecLabel, encoder, decoder) <- codecs; (jsonLabel, json) <- jsons) {
      val label = "codec[%s] json[%s]" format(codecLabel, jsonLabel)
      val jsonWithGarbage = garbage ++ json ++ garbage
      val bytes = encoder.transcode(jsonWithGarbage, garbage.size, json.size)
      val parsed = decoder.decode(bytes)
      val parsedOmni = omniDecoder.decode(bytes)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === Map(), label)
      assert(parsedOmni.objects === dicts, label)
      assert(parsedOmni.header === Map(), label)
    }
  }

  test("Decoding JSON from partial buffers") {
    for ((jsonLabel, json) <- jsons) {
      val jsonWithGarbage = garbage ++ json ++ garbage
      val parsedOmni = omniDecoder.decode(jsonWithGarbage, garbage.size, json.size)
      assert(parsedOmni.objects === dicts, jsonLabel)
      assert(parsedOmni.header === Map(), jsonLabel)
    }
  }

  test("Decoding empty JSON from partial buffers") {
    for ((jsonLabel, json) <- emptyBuffers) {
      val jsonWithGarbage = garbage ++ json ++ garbage
      val parsedOmni = omniDecoder.decode(jsonWithGarbage, garbage.size, json.size)
      assert(parsedOmni.objects === Nil, jsonLabel)
      assert(parsedOmni.header === Map(), jsonLabel)
    }
  }

  test("supportsHeaders correctness") {
    assert(plainJsonEncoder.supportsHeaders === false)
    assert(smileLz4Encoder.supportsHeaders === true)
    assert(jsonLz4Encoder.supportsHeaders === true)
  }

  test("Transcoding with headers") {
    val header = Map("ts" -> new DateTime("2258").getMillis)

    // Plain JSON codec doesn't support headers, so skip it.
    for ((codecLabel, encoder, decoder) <- codecs; (jsonLabel, json) <- jsons if codecLabel != "json-plain") {
      val label = "codec[%s] json[%s]" format(codecLabel, jsonLabel)
      val bytes = encoder.transcode(json, header)
      val parsed = decoder.decode(bytes)
      val parsedOmni = omniDecoder.decode(bytes)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === header, label)
      assert(parsedOmni.objects === dicts, label)
      assert(parsedOmni.header === header, label)
    }
  }

  test("Transcoding with headers into JSON (should fail)") {
    val header = Map("ts" -> new DateTime("2258").getMillis)

    for ((jsonLabel, json) <- jsons) {
      val e = intercept[IllegalArgumentException] {
        plainJsonEncoder.transcode(json, header)
      }
      assert(e.getMessage === "requirement failed: headers not supported")
    }
  }

  test("Transcoding with headers from partial buffers") {
    val header = Map("ts" -> new DateTime("2258").getMillis)

    // Plain JSON codec doesn't support headers, so skip it.
    for ((codecLabel, encoder, decoder) <- codecs; (jsonLabel, json) <- jsons if codecLabel != "json-plain") {
      val label = "codec[%s] json[%s]" format(codecLabel, jsonLabel)
      val jsonWithGarbage = garbage ++ json ++ garbage
      val bytes = encoder.transcode(jsonWithGarbage, garbage.size, json.size, header)
      val parsed = decoder.decode(bytes)
      val parsedOmni = omniDecoder.decode(bytes)
      assert(parsed.objects === dicts, label)
      assert(parsed.header === header, label)
      assert(parsedOmni.objects === dicts, label)
      assert(parsedOmni.header === header, label)
    }
  }

  test("Transcoding emptyish buffers") {
    for ((codecLabel, encoder, decoder) <- codecs; (bufferLabel, buffer) <- emptyBuffers) {
      val label = "codec[%s] json[%s]" format(codecLabel, bufferLabel)
      val bytes = encoder.transcode(buffer)
      val parsed = decoder.decode(bytes)
      val parsedOmni = omniDecoder.decode(bytes)
      assert(parsed.objects === Nil, label)
      assert(parsed.header === Map(), label)
      assert(parsedOmni.objects === Nil, label)
      assert(parsedOmni.header === Map(), label)
    }
  }

  test("Decoding emptyish buffers") {
    for ((label, buffer) <- emptyBuffers) {
      val parsed = omniDecoder.decode(buffer)
      assert(parsed.objects === Nil, label)
      assert(parsed.header === Map(), label)
    }
  }

  test("Smile-LZ4 backwards compatibility") {
    val parsed = omniDecoder.decode(smileLz4Block)
    assert(parsed.objects === dicts)
    assert(parsed.header === Map())
  }

  test("Smile-LZ4 checksum failure") {
    val bytes = smileLz4Encoder.encode(dicts) map {
      case b if b == 'y'.toByte => 'z'.toByte
      case b => b
    }
    val e = intercept[IllegalStateException] {
      smileLz4Decoder.decode(bytes, 0, bytes.length)
    }
    assert(e.getMessage.matches( """Something doesn't add up.*"""))
  }

  test("Plain JSON decoding failure") {
    for (badJson <- Seq("{")) {
      val e = intercept[Exception] {
        plainJsonDecoder.decode(badJson.getBytes(Charsets.UTF_8))
      }
      assert(e.getMessage.startsWith("Unexpected end-of-input"))
    }
  }

  test("Smile-LZ4 decoding failure") {
    val e = intercept[IllegalStateException] {
      smileLz4Decoder.decode(jsonNewlines)
    }
    assert(e.getMessage === "This buffer isn't very magical.")
  }

  test("Smile LZ4 transcoding failure (invalid JSON)") {
    for (badJson <- Seq("{")) {
      val e = intercept[Exception] {
        smileLz4Encoder.transcode(badJson.getBytes(Charsets.UTF_8))
      }
      assert(e.getMessage.startsWith("Unexpected end-of-input"))
    }
  }

  test("Smile LZ4 decoding with errors") {
    val decoder = new SmileLz4Decoder[Dict](
      Jackson.newObjectMapper(new SmileFactory()),
      LZ4Factory.fastestJavaInstance().decompressor()
    )

    for (badJson <- Seq("", "bogus")) {
      val e = intercept[IllegalStateException] {
        decoder.decode(badJson.getBytes(Charsets.UTF_8))
      }
      assert(e.getMessage === "This buffer isn't very magical.")
    }
  }

  test("Omni decoding detection") {
    for ((jsonLabel, json) <- jsons) {
      val jsonWithGarbage = garbage ++ json ++ garbage
      assert(omniDecoder.detect(json) === true)
      assert(omniDecoder.detect(jsonWithGarbage) === false)
      assert(omniDecoder.detect(json, 0, json.size) === true)
      assert(omniDecoder.detect(jsonWithGarbage, garbage.size, json.size) === true)
    }
  }

  test("Omni decoding of garbage") {
    assert(omniDecoder.detect(garbage, 0, garbage.size) === false)
    assert(omniDecoder.detect(garbage) === false)
    val e = intercept[IllegalStateException] {
      omniDecoder.decode(garbage)
    }
    assert(e.getMessage === "Can't decode these bytes!")
  }

  test("Omni decoding with modules") {
    val funkyDecoder = OmniDecoder.default[FunkyClass](new FunkyModule)
    val funkyObjects = Seq("hey", "what") map (new FunkyClass(_))
    val funkyMaps = Seq("hey", "what") map (x => Map("funkyFoo" -> x))
    for ((label, encoder, _) <- codecs) {
      val funkyResult = funkyDecoder.decode(encoder.encode(funkyMaps))
      assert(funkyResult.objects === funkyObjects, label)
      assert(funkyResult.header === Map(), label)
    }
  }
}
