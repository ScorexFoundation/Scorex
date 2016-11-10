package scorex.core.network

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, SupervisorStrategy}
import akka.io.Tcp
import akka.io.Tcp._
import akka.util.{ByteString, CompactByteString}
import com.google.common.primitives.Ints
import scorex.core.network.message.MessageHandler
import scorex.core.network.peer.PeerManager
import scorex.core.network.peer.PeerManager.{AddToBlacklist, Handshaked}
import scorex.core.serialization.ScorexKryoPool
import scorex.core.utils.ScorexLogging

import scala.util.{Failure, Success}


case class ConnectedPeer(socketAddress: InetSocketAddress, handlerRef: ActorRef) {

  import shapeless.syntax.typeable._

  override def equals(obj: scala.Any): Boolean =
    obj.cast[ConnectedPeer].exists(_.socketAddress.getAddress.getHostAddress == this.socketAddress.getAddress.getHostAddress)
}


case object Ack extends Event


//todo: timeout on Ack waiting
case class PeerConnectionHandler(networkControllerRef: ActorRef,
                                 peerManager: ActorRef,
                                 messagesHandler: MessageHandler,
                                 connection: ActorRef,
                                 remote: InetSocketAddress,
                                 serializer: ScorexKryoPool) extends Actor with Buffering with ScorexLogging {

  import PeerConnectionHandler._

  private val selfPeer = ConnectedPeer(remote, self)

  context watch connection

  override def preStart: Unit = connection ! ResumeReading

  // there is not recovery for broken connections
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def processErrors(stateName: String): Receive = {
    case CommandFailed(w: Write) =>
      log.warn(s"Write failed :$w " + remote + s" in state $stateName")
      //      peerManager ! AddToBlacklist(remote)
      connection ! Close
      connection ! ResumeReading
      connection ! ResumeWriting

    case cc: ConnectionClosed =>
      peerManager ! PeerManager.Disconnected(remote)
      log.info("Connection closed to : " + remote + ": " + cc.getErrorCause + s" in state $stateName")
      context stop self

    case CloseConnection =>
      log.info(s"Enforced to abort communication with: " + remote + s" in state $stateName")
      connection ! Close

    case CommandFailed(cmd: Tcp.Command) =>
      log.info("Failed to execute command : " + cmd + s" in state $stateName")
      connection ! ResumeReading
  }

  private var handshakeGot = false
  private var handshakeSent = false

  private object HandshakeDone

  private def handshake: Receive = ({
    case h: Handshake =>
      connection ! Write(ByteString(serializer.toBytesWithoutClass(h)))
      log.info(s"Handshake sent to $remote")
      handshakeSent = true
      if (handshakeGot && handshakeSent) self ! HandshakeDone

    case Received(data) =>
      serializer.fromBytes(data.toArray, classOf[Handshake]) match {
        case Success(handshake) =>
          peerManager ! Handshaked(remote, handshake)
          log.info(s"Got a Handshake from $remote")
          connection ! ResumeReading
          handshakeGot = true
          if (handshakeGot && handshakeSent) self ! HandshakeDone
        case Failure(t) =>
          log.info(s"Error during parsing a handshake", t)
          //todo: blacklist?
          connection ! Close
      }

    case HandshakeDone =>
      connection ! ResumeReading
      context become workingCycle
  }: Receive) orElse processErrors(CommunicationState.AwaitingHandshake.toString)


  def workingCycleLocalInterface: Receive = {
    case msg: message.Message[_] =>
      val bytes = serializer.toBytesWithoutClass(msg)
      log.info("Send message " + msg.spec + " to " + remote)
      connection ! Write(ByteString(Ints.toByteArray(bytes.length) ++ bytes))

    case Blacklist =>
      log.info(s"Going to blacklist " + remote)
      peerManager ! AddToBlacklist(remote)
      connection ! Close
  }

  private var chunksBuffer: ByteString = CompactByteString()

  def workingCycleRemoteInterface: Receive = {
    case Received(data) =>

      val t = getPacket(chunksBuffer ++ data)
      chunksBuffer = t._2

      t._1.find { packet =>
        messagesHandler.parseBytes(packet.toByteBuffer, Some(selfPeer)) match {
          case Success(message) =>
            log.info("received message " + message.spec + " from " + remote)
            networkControllerRef ! message
            false

          case Failure(e) =>
            log.info(s"Corrupted data from: " + remote, e)
            //  connection ! Close
            //  context stop self
            true
        }
      }
      connection ! ResumeReading
  }

  def workingCycle: Receive =
    workingCycleLocalInterface orElse
      workingCycleRemoteInterface orElse
      processErrors(CommunicationState.WorkingCycle.toString) orElse ({
      case nonsense: Any =>
        log.warn(s"Strange input for PeerConnectionHandler: $nonsense")
    }: Receive)

  override def receive: Receive = handshake
}

object PeerConnectionHandler {

  private object CommunicationState extends Enumeration {
    type CommunicationState = Value

    val AwaitingHandshake = Value("AwaitingHandshake")
    val WorkingCycle = Value("WorkingCycle")
  }

  case object CloseConnection

  case object Blacklist

}
