package com.sec.eeg.ars.actor

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.HttpOriginRange
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import ch.megard.akka.http.cors.scaladsl.model.HttpHeaderRange
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import com.sec.eeg.ars.actor.Master.ShutDown
import com.sec.eeg.ars.data._
import org.slf4j.LoggerFactory
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object HttpEndPoint {
  implicit val system = Master.system
  implicit val askTimeout : Timeout = 5 seconds
  implicit val actorMaterializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  implicit val itemFormat1 = jsonFormat2(HTTPRETURN)
  implicit val itemFormat2 = jsonFormat3(DIRRETURN)
  implicit val itemFormat3 = jsonFormat3(IMGRETURN)

  val settings = CorsSettings.defaultSettings.withAllowGenericHttpRequests(true).withAllowedOrigins(HttpOriginRange.*).withAllowedHeaders(HttpHeaderRange.*)

  val route = cors(settings) {
    path("EARS" / "Snapshot") {
      post {
        (headerValueByName("FuncType") & entity(as[String])){ (functype,json) =>
          functype match {
            case "UPLOAD" =>
              onComplete((system.actorSelection("/user/Master/WriteWorker") ? (json)).mapTo[HTTPRETURN]) {
                case Success(value) => complete(value)
                case Failure(ex) => complete(ex)
              }
            case "DIR" =>
              onComplete((system.actorSelection("/user/Master/ReadWorker") ? (functype,json)).mapTo[DIRRETURN]) {
                case Success(value) => complete(value)
                case Failure(ex) => complete(ex)
              }
            case "GETFILE" =>
              onComplete((system.actorSelection("/user/Master/ReadWorker") ? (functype,json)).mapTo[IMGRETURN]) {
                case Success(value) => complete(value)
                case Failure(ex) => complete(ex)
              }
          }
        }
      }
    } ~
    path("EARS" / "kill") {
      post {
        system.actorSelection("/user/Master") ! ShutDown()
        complete("")
      }
    }
  }
}

class HttpEndPoint extends Actor {
  import HttpEndPoint._
  private val log = LoggerFactory.getLogger(classOf[HttpEndPoint])

  override def preStart() : Unit = {
    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", ServiceConfig.HttpPort)
    log.info(s"HttpEndPoint preStart")
  }

  override def postStop() : Unit = {
    Http().shutdownAllConnectionPools()
    log.info(s"HttpEndPoint postStop")
  }

  override def receive = {
    case _ =>
  }
}
