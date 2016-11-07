package scorex.core.network

import java.net.InetSocketAddress

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import scorex.core.network.NetworkController.{DataFromPeer, SendToNetwork}
import scorex.core.network.message.{PeersSpec, GetPeersSpec, Message}
import scorex.core.network.peer.PeerManager
import scorex.core.network.peer.PeerManager.RandomPeers
import scorex.core.utils.ScorexLogging
import shapeless.syntax.typeable._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class PeerSynchronizer(val networkControllerRef: ActorRef, peerManager: ActorRef) extends ViewSynchronizer with ScorexLogging {

  private implicit val timeout = Timeout(5.seconds)

  override val messageSpecs = Seq(GetPeersSpec, PeersSpec)

  override def preStart: Unit = {
    super.preStart()

    val msg = Message[Unit](GetPeersSpec, Right(Unit), None)
    val stn = NetworkController.SendToNetwork(msg, SendToRandom)
    context.system.scheduler.schedule(2.seconds, 10.seconds)(networkControllerRef ! stn)
  }

  override def receive: Receive = {
    case DataFromPeer(spec, peers: Seq[InetSocketAddress]@unchecked, remote)
      if spec.messageCode == PeersSpec.messageCode && peers.cast[Seq[InetSocketAddress]].isDefined =>

      peers.foreach(isa => peerManager ! PeerManager.AddOrUpdatePeer(isa, None, None))

    case DataFromPeer(spec, _, remote) if spec.messageCode == GetPeersSpec.messageCode =>

      //todo: externalize the number, check on receiving
      (peerManager ? RandomPeers(3))
        .mapTo[Seq[InetSocketAddress]]
        .foreach { peers =>
          val msg = Message(PeersSpec, Right(peers), None)
          networkControllerRef ! SendToNetwork(msg, SendToPeers(Seq(remote)))
        }

    case nonsense: Any => log.warn(s"PeerSynchronizer: got something strange $nonsense")
  }
}
