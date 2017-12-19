package scorex.core.network.message

import java.nio.ByteBuffer

import scorex.core.network.ConnectedPeer
import scorex.crypto.hash.Blake2b256

import scala.language.existentials
import scala.util.Try


case class MessageHandler(specs: Seq[MessageSpec[_]]) {

  import Message._

  private val specsMap = {
    val m = Map(specs.map(s => s.messageCode -> s): _*)
    require(m.size == specs.size, "Duplicate message codes")
    m
  }

  //MAGIC ++ Array(spec.messageCode) ++ Ints.toByteArray(dataLength) ++ dataWithChecksum
  def parseBytes(bytes: ByteBuffer, sourceOpt: Option[ConnectedPeer]): Try[Message[_]] = Try {
    val magic = new Array[Byte](MagicLength)
    bytes.get(magic)

    require(magic.sameElements(Message.MAGIC), "Wrong magic bytes" + magic.mkString)

    val msgCode = bytes.get

    val length = bytes.getInt
    require(length >= 0, "Data length is negative!")

    val msgData: Array[Byte] = if (length > 0) {
      val data = new Array[Byte](length)
      //READ CHECKSUM
      val checksum = new Array[Byte](Message.ChecksumLength)
      bytes.get(checksum)

      //READ DATA
      bytes.get(data)

      //VALIDATE CHECKSUM
      val digest = Blake2b256.hash(data).take(Message.ChecksumLength)

      //CHECK IF CHECKSUM MATCHES
      assert(checksum.sameElements(digest), s"Invalid data checksum length = $length")
      data
    }
    else Array()

    val spec = {
      val s = specsMap.get(msgCode)
      require(s.isDefined, s"No message handler found for $msgCode")
      s.get
    }

    Message(spec, Left(msgData), sourceOpt)
  }
}
