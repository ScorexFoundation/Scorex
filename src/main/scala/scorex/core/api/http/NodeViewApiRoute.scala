package scorex.core.api.http

import akka.actor.{ActorRef, ActorRefFactory}
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import io.circe.syntax._
import scorex.core.NodeViewHolder.{CurrentView, GetDataFromCurrentView}
import scorex.core.consensus.History
import scorex.core.network.ConnectedPeer
import scorex.core.settings.RESTApiSettings
import scorex.core.transaction.box.proposition.Proposition
import scorex.core.transaction.state.MinimalState
import scorex.core.transaction.wallet.Vault
import scorex.core.transaction.{MemoryPool, Transaction}
import scorex.core.{ModifierId, PersistentNodeViewModifier}
import scorex.crypto.encode.Base58

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


case class NodeViewApiRoute[P <: Proposition, TX <: Transaction[P]]
(override val settings: RESTApiSettings, nodeViewHolderRef: ActorRef)
(implicit val context: ActorRefFactory) extends ApiRoute {

  override val route = pathPrefix("nodeView") {
    openSurface ~ persistentModifierById ~ pool
  }

  type PM <: PersistentNodeViewModifier
  type HIS <: History[PM, _, _ <: History[PM, _, _]]
  type MP <: MemoryPool[TX, _ <: MemoryPool[TX, _]]
  type MS <: MinimalState[PM, _ <: MinimalState[_, _]]
  type VL <: Vault[P, TX, PM, _ <: Vault[P, TX, PM, _]]

  case class OpenSurface(ids: Seq[ModifierId])

  def getOpenSurface(): Try[OpenSurface] = Try {
    def f(v: CurrentView[HIS, MS, VL, MP]): OpenSurface = OpenSurface(v.history.openSurfaceIds())

    Await.result(nodeViewHolderRef ? GetDataFromCurrentView(f), 5.seconds).asInstanceOf[OpenSurface]
  }

  case class MempoolData(size: Int, transactions: Iterable[TX])

  def getMempool(limit: Int = 1000): Try[MempoolData] = Try { //scalastyle:ignore magic.number
    def f(v: CurrentView[HIS, MS, VL, MP]): MempoolData = MempoolData(v.pool.size, v.pool.take(limit))

    Await.result(nodeViewHolderRef ? GetDataFromCurrentView(f), 5.seconds).asInstanceOf[MempoolData]
  }

  def pool: Route = path("pool") {
    getJsonRoute {
      getMempool() match {
        case Success(mpd: MempoolData) => SuccessApiResponse(
          Map(
            "size" -> mpd.size.asJson,
            "transactions" -> mpd.transactions.map(_.json).asJson
          ).asJson
        )
        case Failure(e) => ApiException(e)
      }
    }
  }

  def openSurface: Route = path("openSurface") {
    getJsonRoute {
      getOpenSurface() match {
        case Success(os: OpenSurface) => SuccessApiResponse(os.ids.map(Base58.encode).asJson)
        case Failure(e) => ApiException(e)
      }
    }
  }

  def persistentModifierById: Route = path("persistentModifier" / Segment) { encodedId =>
    getJsonRoute {
      Base58.decode(encodedId) match {
        case Success(rawId) =>
          val id = ModifierId @@ rawId

          def f(v: CurrentView[HIS, MS, VL, MP]): Option[PM] = v.history.modifierById(id)

          (nodeViewHolderRef ? GetDataFromCurrentView[HIS, MS, VL, MP, Option[PM]](f)).mapTo[Option[PM]]
            .map(_.map(tx => SuccessApiResponse(tx.json)).getOrElse(ApiError.notExists))
        case _ => Future(ApiError.notExists)
      }
    }
  }

}
