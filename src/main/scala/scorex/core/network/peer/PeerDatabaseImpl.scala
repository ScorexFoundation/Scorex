package scorex.core.network.peer

import java.net.{InetSocketAddress, NetworkInterface}

import scorex.core.settings.NetworkSettings
import scorex.core.utils.NetworkTimeProvider

import scala.collection.mutable
import scala.collection.JavaConverters._


//todo: persistence
class PeerDatabaseImpl(settings: NetworkSettings,
                       filename: Option[String],
                       timeProvider: NetworkTimeProvider) extends PeerDatabase {

  private val whitelistPersistence = mutable.Map[InetSocketAddress, PeerInfo]()

  private val blacklist = mutable.Map[String, Long]()

  override def addOrUpdateKnownPeer(address: InetSocketAddress, peerInfo: PeerInfo): Unit = {
    val updatedPeerInfo = whitelistPersistence.get(address).map { dbPeerInfo =>
      val nodeNameOpt = peerInfo.nodeName.orElse(dbPeerInfo.nodeName)
      PeerInfo(peerInfo.lastSeen, nodeNameOpt)
    }.getOrElse(peerInfo)
    whitelistPersistence.put(address, updatedPeerInfo)
  }

  override def blacklistPeer(address: InetSocketAddress): Unit = {
    whitelistPersistence.remove(address)
    if (!isBlacklisted(address)) blacklist += address.getHostName -> timeProvider.time()
  }

  override def isBlacklisted(address: InetSocketAddress): Boolean = {
    blacklist.synchronized(blacklist.contains(address.getHostName))
  }

  override def knownPeers(excludeSelf: Boolean): Map[InetSocketAddress, PeerInfo] = {
    if (excludeSelf) {
      val localAddresses = if (settings.bindAddress.getAddress.isAnyLocalAddress) {
        NetworkInterface.getNetworkInterfaces.asScala
          .flatMap(_.getInetAddresses.asScala
            .map(a => new InetSocketAddress(a, settings.bindAddress.getPort)))
          .toSet
      } else Set(settings.bindAddress)

      val excludedAddresses = localAddresses ++ settings.declaredAddress.toSet
      knownPeers(false).filterKeys(!excludedAddresses(_))
    } else {
      whitelistPersistence.keys.flatMap(k => whitelistPersistence.get(k).map(v => k -> v)).toMap
    }

  }

  override def blacklistedPeers(): Seq[String] = blacklist.keys.toSeq

  override def isEmpty(): Boolean = whitelistPersistence.isEmpty
}
