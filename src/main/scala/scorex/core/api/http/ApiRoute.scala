package scorex.core.api.http

import akka.actor.ActorRefFactory
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.PredefinedFromEntityUnmarshallers
import akka.util.Timeout
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Printer
import scorex.core.utils.{ActorHelper, ScorexLogging}
import scala.language.implicitConversions

trait ApiRoute
  extends ApiDirectives
    with ActorHelper
    with FailFastCirceSupport
    with PredefinedFromEntityUnmarshallers
    with ScorexLogging {

  def context: ActorRefFactory
  def route: Route

  //TODO: should we move it to the settings?
  override val apiKeyHeaderName: String = "api_key"

  implicit def httpJsonStatus(status: StatusCode): ApiResponse = ApiResponse(status)
  implicit val printer: Printer = ApiResponse.printer
  implicit lazy val timeout: Timeout = Timeout(settings.timeout)

}
