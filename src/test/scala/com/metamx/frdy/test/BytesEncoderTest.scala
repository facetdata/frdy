package com.metamx.frdy.test

import com.fasterxml.jackson.dataformat.smile.{SmileFactory, SmileGenerator, SmileParser}
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef.EffectOps
import com.metamx.common.scala.untyped._
import com.metamx.frdy.codec._
import org.scalatest.FunSuite

class BytesEncoderTest extends FunSuite
{
  val smileFactory = new SmileFactory().withEffect { sf =>
    sf.disable(SmileGenerator.Feature.WRITE_HEADER)
    sf.disable(SmileParser.Feature.REQUIRE_HEADER)
  }
  val plainBytesEncoder = new PlainBytesEncoder(Jackson.newObjectMapper(smileFactory))
  val plainBytesDecoder = new PlainBytesDecoder(Jackson.newObjectMapper(smileFactory))

  val bytesOmniDecoder = new OmniDecoder[Array[Byte]](
    plainBytesDecoder
  )

  val timestampDict = Dict("t" -> 1458341270956L)

  val message = "Some random message".getBytes("UTF-8")

  val encoded = Array[Byte](
    -16, -97, -104, -76, 4, 0, 0, 0, 2, -6, -5, 83, 111, 109, 101, 32, 114, 97, 110, 100, 111, 109, 32, 109,
    101, 115, 115, 97, 103, 101)

  val encodedWithDict= Array[Byte](
    -16, -97, -104, -76, 4, 0, 0, 0, 12, -6, -128, 116, 37, 1, 41, 98, 125, 29, 45, -104, -5, 83, 111, 109,
    101, 32, 114, 97, 110, 100, 111, 109, 32, 109, 101, 115, 115, 97, 103, 101)


  val encodedVariants = Map(
    "single message" -> (encoded, FrdyResult(Seq(message), Map.empty)),
    "single message with timestamp" -> (encodedWithDict,FrdyResult(Seq(message), timestampDict))

  )

  val frdyBlockWrongMagic = Array[Byte](
    15, -97, -104, -76,
    4,
    0, 0, 0, 2, 0, 0, 0, 0, 123, 125,
    77, -119, 11, -21
  )
  val frdyBlockWrongVersion = Array[Byte](
    -16, -97, -104, -76,
    2,
    0, 0, 0, 2, 0, 0, 0, 0, 123, 125,
    -88, 17, -98, -63
  )

  val badBlocks = Map(
    "wrong magic" ->(frdyBlockWrongMagic, "This buffer isn't encoded with PlainBytesEncoder"),
    "wrong version" ->(frdyBlockWrongVersion, "This buffer isn't encoded with PlainBytesEncoder")
  )

  test("Single message round-trip") {
    val frdyResult = plainBytesDecoder.decode(plainBytesEncoder.encode(message))
    assert(frdyResult.objects.head.deep === message.deep)
    assert(frdyResult.header === Map.empty)
  }

  test("Single message round-trip via OmniDecoder") {
    val frdyResult = bytesOmniDecoder.decode(plainBytesEncoder.encode(message))
    assert(frdyResult.objects.head.deep === message.deep)
    assert(frdyResult.header === Map.empty)
  }

  test("Single message with timestamp round-trip") {
    val frdyResult = plainBytesDecoder.decode(plainBytesEncoder.encode(message, timestampDict))
    assert(frdyResult.objects.head.deep === message.deep)
    assert(frdyResult.header === timestampDict)
  }

  test("Decode bad block") {
    badBlocks.foreach {
      case (label, (badBlock, expectedErrorMessage)) =>
        val e = intercept[IllegalStateException](plainBytesDecoder.decode(badBlock))
        assert(e.getMessage === expectedErrorMessage, label)
    }
  }

  test("Decode previously encoded message") {
    //Code used to generate encoded messages:
    //    val encoded = plainBytesEncoder.transcode(message)
    //    println("val encoded = " + encoded.deep)
    //    val encoded2 = plainBytesEncoder.transcode(message, timestampDict)
    //    println("val encodedWithDict= " + encoded2.deep)

    encodedVariants.foreach {
      case (label, (block, expectedResult)) =>
        val decoded = bytesOmniDecoder.decode(block)
        assert(expectedResult.objects.map(_.deep) === decoded.objects.map(_.deep), label)
        assert(decoded.header === expectedResult.header, label)
    }
  }

}
