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

import com.metamx.common.scala.Predef._
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * Utilities for dealing with FRDY blocks. FRDY blocks have the structure:
 *
 * 1) 4 bytes of magic
 * 2) 1 byte version number
 * 3) data section
 * 4) 4 byte CRC32 checksum of the entire block, little-endian order, not counting these 4 bytes
 */
object FrdyContainer
{
  val IntSize           : Int          = 4
  val VersionSize       : Int          = 1
  val Magic             : Array[Byte]  = Array(0xf0, 0x9f, 0x98, 0xb4) map (_.toByte)
  val ChecksumSize      : Int          = 4 /* CRC32 */
  val ContainerOverhead : Int          = Magic.size + VersionSize + ChecksumSize

  /**
   * Checks that container starts with magic bytes followed by version
   */
  def detectMagicAndVersion(bytes: Array[Byte], offset: Int, version: Int): Boolean = {
    bytes(offset) == Magic(0) &&
      bytes(offset + 1) == Magic(1) &&
      bytes(offset + 2) == Magic(2) &&
      bytes(offset + 3) == Magic(3) &&
      bytes(offset + 4) == version
  }

  /**
   * Creates a FRDY block.
   */
  def createBlock(version: Byte, data: Array[Byte], dataOffset: Int, dataLength: Int): Array[Byte] = {
    val block = ByteBuffer.allocate(
      Magic.size + VersionSize + dataLength + ChecksumSize
    )
    block.put(Magic).put(version).put(data, dataOffset, dataLength)
    val crc32 = new CRC32 withEffect {
      crc32 =>
        crc32.update(block.array(), block.arrayOffset(), block.position())
    }
    writeChecksumToByteBuffer(crc32, block)
    assert(block.position() == block.limit())
    block.array()
  }

  /**
   * Preferred way to create a FRDY block in case data length is known in advance.
   * Than use returned buffer to write message data in and finally use finalizeBlockWithChecksum to add checksum.
   */
  def prepareBlock(version:Byte, dataLength:Int, withChecksum:Boolean = true) = {
    val block = ByteBuffer.allocate(
      Magic.size + VersionSize + dataLength + (if (withChecksum) ChecksumSize else 0)
    )
    block.put(Magic).put(version)
  }

  def finalizeBlockWithChecksum(block:ByteBuffer) = {
    val crc32 = new CRC32 withEffect {
      crc32 =>
        crc32.update(block.array(), block.arrayOffset(), block.position())
    }
    writeChecksumToByteBuffer(crc32, block)
    assert(block.position() == block.limit())
    block.array()
  }

  /**
    * Returns tuple (data offset,  data length) inside FRDY block
    */
  def extractData(blockOffset: Int, blockLength: Int, withChecksum:Boolean = true): (Int, Int) = {
    val dataOffset = blockOffset + Magic.size + VersionSize
    val dataLength = blockLength - Magic.size - VersionSize - (if (withChecksum) ChecksumSize else 0)
    (dataOffset, dataLength)
  }

  def writeChecksumToByteBuffer(crc32: CRC32, buf: ByteBuffer) {
    val value = crc32.getValue
    buf.put((value & 0xFF).toByte)
    buf.put(((value >> 8) & 0xFF).toByte)
    buf.put(((value >> 16) & 0xFF).toByte)
    buf.put(((value >> 24) & 0xFF).toByte)
  }

  def verifyChecksum(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    val expectedChecksumBytes = (ByteBuffer.allocate(ChecksumSize) withEffect {
      buf =>
        val crc32 = new CRC32 withEffect {
          crc32 =>
            crc32.update(bytes, offset, length - ChecksumSize)
        }
        writeChecksumToByteBuffer(crc32, buf)
    }).array()
    val checksumBytes = bytes.slice(offset + length - ChecksumSize, offset + length)
    if (expectedChecksumBytes.deep != checksumBytes.deep) {
      throw new IllegalStateException(
        "Something doesn't add up... computed checksum[%s] but read checksum[%s]." format(
          expectedChecksumBytes.map("%02x" format _).mkString,
          checksumBytes.map("%02x" format _).mkString
          )
      )
    }
  }
}
