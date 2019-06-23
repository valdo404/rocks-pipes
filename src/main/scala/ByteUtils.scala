import scodec.bits.ByteVector

object ByteUtils {
  def toString(c: Array[Byte]) = {
    ByteVector(c).decodeUtf8.toOption
  }
}
