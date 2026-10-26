package com.sec.eeg.ars.actor

import akka.actor.{Actor, ActorRef}
import com.sec.eeg.ars.data._
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory

class TransformerMsg(rRouter: ActorRef, wRouter: ActorRef) extends Actor {
  private val log = LoggerFactory.getLogger(classOf[TransformerMsg])
  implicit val formats = DefaultFormats
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  override def receive : Receive = {
    case msg : (String, String) =>
      msg._1 match {
        case "UPLOAD" =>
          val data = parse(msg._2)
          val conv = data.extract[UPLOADFILES]
          log.info(s"received UPLOAD: ${conv.hostname}, ${conv.filename}")
          wRouter.forward(conv)
          sender() ! HTTPRETURN("Success", "OK")
        case "DIR" =>
          val data = parse(msg._2)
          val conv = data.extract[DIRSNAPSHOT]
          log.info(s"received DIR: ${conv.hostname}, ${conv.year}, ${conv.month}")
          rRouter.forward(conv)
        case "GETFILE" =>
          val data = parse(msg._2)
          val conv = data.extract[GETFILE]
          log.info(s"received GETFILE: ${conv.hostname}, ${conv.filename}, ${conv.year}, ${conv.month}")
          rRouter.forward(conv)
        case m =>
          log.warn(s"Unhandled message: ${m}")
      }
  }

  override def preStart() : Unit = {
    log.info(s"TransformerMsg preStart")
  }

  override def postStop() : Unit = {
    log.info(s"TransformerMsg postStop")
  }
}
