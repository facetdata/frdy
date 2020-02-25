# FRDY

Frdy is a library that helps you decode, encode, and transcode blocks of objects.

## Example

When encoding, you choose which encoder to use. When decoding, you can use an OmniDecoder that detects the encoded
type and uses the appropriate decoder.

```scala
val smileLz4Encoder = new SmileLz4Encoder[Dict](
  Jackson.newObjectMapper(),
  Jackson.newObjectMapper(new SmileFactory()),
  LZ4Factory.fastestJavaInstance().fastCompressor()
)
val decoder = OmniDecoder.default[Dict]()

// Transcoding goes from JSON buffers to encoded blocks.
val transcodedBytes = smileLz4Encoder.transcode("""{"hey": "what"}{"foo": "bar"}""".getBytes)
val dictsFromTranscoding = decoder.decode(transcodedBytes)

// Encoding goes from objects to encoded blocks.
val encodedBytes = smileLz4Encoder.encode(Seq(Map("hey" -> "what"), Map("foo" -> "bar")))
val dictsFromEncoding = decoder.decode(encodedBytes)

// The OmniDecoder can also read plain JSON.
val dictsFromJson = decoder.decode("""{"hey": "what"}{"foo": "bar"}""".getBytes)
```

## Block format

Frdy containers have the structure:

1. 4 bytes of magic
2. 1 byte version number
3. data section
4. 4 byte CRC32 checksum of the entire block, not counting these 4 bytes

Jackson-LZ4 (both Json-LZ4 and Smile-LZ4) data sections have the structure:

1. 4 byte decompressed header length
2. 4 byte decompressed payload length
3. LZ4-compressed concatenated header and payload, encoded with Jackson

Headers are a single object. Payloads can either be a series of root objects (like ```{}{}```) or a single root array
of objects (like ```[{},{}]```).

## Plain JSON

Plain JSON encoding and decoding is supported too. Plain JSON blocks are simply text, and have no container format.
In particular, there can be no headers nor checksums. Plain JSON blocks can either be a series of root objects or a
single root array of objects.

## Performance

Relative transcoding performance, on my machine. These tests all started with a JSON buffer, and:

- t_ser is the time taken to convert the JSON buffer to the an encoded block in the target format
- bytes is the size of the encoded block
- t_deser is the time taken to decode those bytes back to objects

```
codec                                   t_ser    bytes  t_deser
=====                                   =====    =====  =======
json                                        0   984835    5564
json-gzip                               23119   203102    8649
json-snappy                              3354   394939    6276
json-lz4-fast                            2409   309046    6150
json-lz4-high                           21346   212291    5992
frdy-json-lz4-trans-fast                 2931   309043    6618
frdy-json-lz4-trans-high                21801   212310    5980
frdy-json-lz4-decenc-fast               12213   308204    6027
frdy-json-lz4-decenc-high               32299   212624    6125
frdy-smile-lz4-trans-fast                6882   257561    4561
frdy-smile-lz4-trans-fast-shared         8340   257836    4400
frdy-smile-lz4-trans-high               18287   188394    4712
frdy-smile-lz4-decenc-fast              12996   258047    7104
frdy-smile-lz4-decenc-fast-shared       11891   256724    4350
frdy-smile-lz4-decenc-high              22561   188513    4506
```

Relative encoding performance, on my machine. These tests all started with a set of Java objects, and:

- t_ser is the time taken to encode the objects to the target format
- bytes is the size of the encoded block
- t_deser is the time taken to decode those bytes back to objects

```
codec                                   t_ser    bytes  t_deser
=====                                   =====    =====  =======
frdy-json-lz4-enc-fast                   7291   308204     6172
frdy-json-lz4-enc-high                  26328   212624     5978
frdy-smile-lz4-enc-fast                  5507   258047     4613
frdy-smile-lz4-enc-fast-shared           6055   256724     4523
frdy-smile-lz4-enc-high                 17753   188513     4665
```
