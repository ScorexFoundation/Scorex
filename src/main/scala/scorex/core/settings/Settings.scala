package scorex.core.settings

import java.io.File
import java.net.InetSocketAddress

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import scorex.core.network.message.Message
import scorex.core.utils.NetworkTimeProviderSettings
import scorex.util.ScorexLogging

import scala.concurrent.duration._

case class RESTApiSettings(bindAddress: InetSocketAddress,
                           apiKeyHash: Option[String],
                           corsAllowedOrigin: Option[String],
                           timeout: FiniteDuration)

case class NetworkSettings(nodeName: String,
                           addedMaxDelay: Option[FiniteDuration],
                           localOnly: Boolean,
                           knownPeers: Seq[InetSocketAddress],
                           bindAddress: InetSocketAddress,
                           maxConnections: Int,
                           connectionTimeout: FiniteDuration,
                           upnpEnabled: Boolean,
                           upnpGatewayTimeout: Option[FiniteDuration],
                           upnpDiscoverTimeout: Option[FiniteDuration],
                           declaredAddress: Option[InetSocketAddress],
                           handshakeTimeout: FiniteDuration,
                           deliveryTimeout: FiniteDuration,
                           maxDeliveryChecks: Int,
                           appVersion: String,
                           agentName: String,
                           maxPacketSize: Int,
                           maxHandshakeSize: Int,
                           maxInvObjects: Int,
                           desiredInvObjects: Int,
                           syncInterval: FiniteDuration,
                           syncStatusRefresh: FiniteDuration,
                           syncIntervalStable: FiniteDuration,
                           syncStatusRefreshStable: FiniteDuration,
                           inactiveConnectionDeadline: FiniteDuration,
                           syncTimeout: Option[FiniteDuration],
                           controllerTimeout: Option[FiniteDuration],
                           maxModifiersCacheSize: Int,
                           magicBytes: Array[Byte],
                           getPeersInterval: FiniteDuration,
                           maxPeerSpecObjects: Int,
                           temporalBanDuration: FiniteDuration,
                           penaltySafeInterval: FiniteDuration,
                           penaltyScoreThreshold: Int,
                           peerEvictInterval: FiniteDuration)

case class ScorexSettings(dataDir: File,
                          logDir: File,
                          network: NetworkSettings,
                          restApi: RESTApiSettings,
                          ntp: NetworkTimeProviderSettings)


object ScorexSettings extends ScorexLogging with SettingsReaders {

  protected val configPath: String = "scorex"

  def readConfigFromPath(userConfigPath: Option[String], configPath: String): Config = {

    val maybeConfigFile: Option[File] = userConfigPath.map(filename => new File(filename)).filter(_.exists())
      .orElse(userConfigPath.flatMap(filename => Option(getClass.getClassLoader.getResource(filename))).
        map(r => new File(r.toURI)).filter(_.exists()))

    val config = maybeConfigFile match {
      // if no user config is supplied, the library will handle overrides/application/reference automatically
      case None =>
        log.warn("NO CONFIGURATION FILE WAS PROVIDED. STARTING WITH DEFAULT SETTINGS FOR TESTNET!")
        ConfigFactory.load()
      // application config needs to be resolved wrt both system properties *and* user-supplied config.
      case Some(file) =>
        val cfg = ConfigFactory.parseFile(file)
        if (!cfg.hasPath(configPath)) {
          throw new Error("Malformed configuration file was provided! Aborting!")
        }
        ConfigFactory
          .defaultOverrides()
          .withFallback(cfg) // user-supplied config
          .withFallback(ConfigFactory.defaultApplication())
          .withFallback(ConfigFactory.defaultReference()) // "src/main/resources/reference.conf"
          .resolve()
    }

    config
  }

  def read(userConfigPath: Option[String]): ScorexSettings = {
    fromConfig(readConfigFromPath(userConfigPath, configPath))
  }

  def fromConfig(config: Config): ScorexSettings = {
    config.as[ScorexSettings](configPath)
      .ensuring(_.network.magicBytes.length == Message.MagicLength)
  }
}
