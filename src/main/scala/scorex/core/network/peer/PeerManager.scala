package scorex.core.network.peer

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import scorex.core.app.ScorexContext
import scorex.core.network._
import scorex.core.settings.ScorexSettings
import scorex.core.utils.NetworkUtils
import scorex.util.ScorexLogging

import scala.util.Random

/**
  * Peer manager takes care of peers connected and in process, and also chooses a random peer to connect
  * Must be singleton
  */
class PeerManager(settings: ScorexSettings, scorexContext: ScorexContext) extends Actor with ScorexLogging {

  import PeerManager.ReceivableMessages._

  private val peerDatabase = new InMemoryPeerDatabase(settings.network, scorexContext.timeProvider)

  if (peerDatabase.isEmpty) {
    // fill database with peers from config file if empty
    settings.network.knownPeers.foreach { address =>
      if (!isSelf(address)) {
        peerDatabase.addOrUpdateKnownPeer(PeerInfo.fromAddress(address))
      }
    }
  }

  override def receive: Receive = peersManagement orElse apiInterface orElse {
    case a: Any =>
      log.error(s"Wrong input for peer manager: $a")
  }

  private def peersManagement: Receive = {

    case ConfirmConnection(connectionId, handlerRef) =>
      log.info(s"Connection confirmation request: $connectionId")
      if (peerDatabase.isBlacklisted(connectionId.remoteAddress)) sender() ! ConnectionDenied(connectionId, handlerRef)
      else sender() ! ConnectionConfirmed(connectionId, handlerRef)

    case AddOrUpdatePeer(peerInfo) =>
      // We have connected to a peer and got his peerInfo from him
      if (!isSelf(peerInfo.peerSpec)) peerDatabase.addOrUpdateKnownPeer(peerInfo)

    case Penalize(peer, penaltyType) =>
      log.info(s"$peer penalized, penalty: $penaltyType")
      if (peerDatabase.penalize(peer, penaltyType)) {
        log.info(s"$peer blacklisted")
        peerDatabase.addToBlacklist(peer, penaltyType)
        sender() ! Blacklisted(peer)
      }

    case AddPeerIfEmpty(peerSpec) =>
      // We have received peer data from other peers. It might be modified and should not affect existing data if any
      if (peerSpec.address.forall(a => peerDatabase.get(a).isEmpty) && !isSelf(peerSpec)) {
        val peerInfo: PeerInfo = PeerInfo(peerSpec, 0, None)
        log.info(s"New discovered peer: $peerInfo")
        peerDatabase.addOrUpdateKnownPeer(peerInfo)
      }

    case RemovePeer(address) =>
      log.info(s"$address removed from peers database")
      peerDatabase.remove(address)

    case get: GetPeers[_] =>
      sender() ! get.choose(peerDatabase.knownPeers, peerDatabase.blacklistedPeers, scorexContext)
  }

  private def apiInterface: Receive = {

    case GetAllPeers =>
      log.trace(s"Get all peers: ${peerDatabase.knownPeers}")
      sender() ! peerDatabase.knownPeers

    case GetBlacklistedPeers =>
      sender() ! peerDatabase.blacklistedPeers
  }

  /**
    * Given a peer's address, returns `true` if the peer is the same is this node.
    */
  private def isSelf(peerAddress: InetSocketAddress): Boolean = {
    NetworkUtils.isSelf(peerAddress, settings.network.bindAddress, scorexContext.externalNodeAddress)
  }

  private def isSelf(peerSpec: PeerSpec): Boolean = {
    peerSpec.declaredAddress.exists(isSelf) || peerSpec.localAddressOpt.exists(isSelf)
  }

}

object PeerManager {

  object ReceivableMessages {

    case class ConfirmConnection(connectionId: ConnectionId, handlerRef: ActorRef)

    case class ConnectionConfirmed(connectionId: ConnectionId, handlerRef: ActorRef)

    case class ConnectionDenied(connectionId: ConnectionId, handlerRef: ActorRef)

    case class Penalize(remote: InetSocketAddress, penaltyType: PenaltyType)

    case class Blacklisted(remote: InetSocketAddress)

    // peerListOperations messages
    case class AddOrUpdatePeer(data: PeerInfo)

    case class AddPeerIfEmpty(data: PeerSpec)

    case class RemovePeer(address: InetSocketAddress)

    /**
      * Message to get peers from known peers map filtered by `choose` function
      */
    trait GetPeers[T] {
      def choose(knownPeers: Map[InetSocketAddress, PeerInfo],
                 blacklistedPeers: Seq[InetAddress],
                 scorexContext: ScorexContext): T
    }

    /**
      * Choose at most `howMany` random peers, which were connected to our peer and weren't blacklisted.
      *
      * Used in peer propagation: peers chosen are recommended to a peer asking our node about more peers.
      */
    case class SeenPeers(howMany: Int) extends GetPeers[Seq[PeerInfo]] {

      override def choose(knownPeers: Map[InetSocketAddress, PeerInfo],
                          blacklistedPeers: Seq[InetAddress],
                          sc: ScorexContext): Seq[PeerInfo] = {
        val recentlySeenNonBlacklisted = knownPeers.values.toSeq
          .filter { p =>
            (p.connectionType.isDefined || p.lastHandshake > 0) &&
              !blacklistedPeers.exists(ip => p.peerSpec.declaredAddress.exists(_.getAddress == ip))
          }
        Random.shuffle(recentlySeenNonBlacklisted).take(howMany)
      }
    }

    case object GetAllPeers extends GetPeers[Map[InetSocketAddress, PeerInfo]] {

      override def choose(knownPeers: Map[InetSocketAddress, PeerInfo],
                          blacklistedPeers: Seq[InetAddress],
                          sc: ScorexContext): Map[InetSocketAddress, PeerInfo] = knownPeers
    }

    // Extract /16 IPv4 prefix (IP group)
    private  def getIpGroup(addr : InetSocketAddress) : Int = {
      val ip = addr.getAddress.getAddress
      val group = ((ip(0) & 0xFF) << 8) | (ip(1) & 0xFF)
      group
    }

    case class RandomPeerExcluding(excludedPeers: Seq[PeerInfo]) extends GetPeers[Option[PeerInfo]] {

      override def choose(knownPeers: Map[InetSocketAddress, PeerInfo],
                          blacklistedPeers: Seq[InetAddress],
                          sc: ScorexContext): Option[PeerInfo] = {
        // First of all try to establish connections to the hosts from different IP group
		// It will complicate eclipse attacks
        val excludedGroups = excludedPeers.flatMap(_.peerSpec.address).map(getIpGroup(_)).toSet
        val allCandidates = knownPeers.values.filterNot { p =>
          excludedPeers.exists(_.peerSpec.address == p.peerSpec.address) ||
          blacklistedPeers.exists(addr => p.peerSpec.address.map(_.getAddress).contains(addr))
        }.toSeq
        val preferredCandidates = allCandidates.filterNot(_.peerSpec.address.fold(true)(addr => excludedGroups.contains(getIpGroup(addr))))
		// If it is not possible to connect to the node from different IP group, then try to connect somewhere
        val candidates = if (preferredCandidates.nonEmpty) preferredCandidates else allCandidates
        if (candidates.nonEmpty) Some(candidates(Random.nextInt(candidates.size)))
        else None
      }
    }

    case object GetBlacklistedPeers extends GetPeers[Seq[InetAddress]] {

      override def choose(knownPeers: Map[InetSocketAddress, PeerInfo],
                          blacklistedPeers: Seq[InetAddress],
                          scorexContext: ScorexContext): Seq[InetAddress] = blacklistedPeers
    }

  }

}

object PeerManagerRef {

  def props(settings: ScorexSettings, scorexContext: ScorexContext): Props = {
    Props(new PeerManager(settings, scorexContext))
  }

  def apply(settings: ScorexSettings, scorexContext: ScorexContext)
           (implicit system: ActorSystem): ActorRef = {
    system.actorOf(props(settings, scorexContext))
  }

  def apply(name: String, settings: ScorexSettings, scorexContext: ScorexContext)
           (implicit system: ActorSystem): ActorRef = {
    system.actorOf(props(settings, scorexContext), name)
  }

}
