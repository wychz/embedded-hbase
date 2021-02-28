package com.iiichz.embeddedhbase

import org.apache.commons.codec.binary.Hex


/** Helper to work with the MD5 value space. */
object MD5Space {
  val Min = BigInt(0)
  val Max = (BigInt(1) << 128) - 1

  private val MaxLong = 0x7fffffffffffffffL

  def apply(pos: Double): Array[Byte] = {
    val hash = Min + ((Max * (pos * MaxLong).toLong) / MaxLong)
    var hashStr = "%032x".format(hash)
    return Hex.decodeHex(hashStr.toCharArray)
  }

  /**
   * Maps a position from a rational number into the MD5 hash space.
   *
   * @param num Numerator (≥ 0 and ≤ denum).
   * @param denum Denumerator (≥ 0).
   * @return MD5 hash as an array of 16 bytes.
   */
  def apply(num: BigInt, denum: BigInt): Array[Byte] = {
    require((num >= 0) && (denum > 0))
    require(num <= denum)
    val hash = Min + ((num * Max) / denum)
    var hashStr = "%032x".format(hash)
    return Hex.decodeHex(hashStr.toCharArray)
  }
}