package com.sec.eeg.ars.actor

import akka.actor.Actor
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

case class ADD_UPLOAD(hostname: String)
case class WRITE_LOG()

class Logger extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  private val log = LoggerFactory.getLogger(classOf[Logger])

  val upload_eqpid = scala.collection.mutable.ListBuffer.empty[String]
  var flush_work : akka.actor.Cancellable = null

  override def preStart() : Unit = {
    flush_work = context.system.scheduler.scheduleOnce(60 seconds, self, WRITE_LOG())
    log.info("Logger preStart")
  }

  override def postStop() : Unit = {
    try {
      if (flush_work != null)
        flush_work.cancel()
    } catch {
      case ex : Throwable =>
    }

    log.info("Logger postStop")
  }

  override def receive : Receive = {
    case ADD_UPLOAD(eqpid) =>
      upload_eqpid.+=(eqpid)
    case WRITE_LOG() =>
      if (upload_eqpid.length > 0) {
        val wordCount = scala.collection.mutable.HashMap[String, Int]().withDefaultValue(0)
        upload_eqpid.foreach(word => wordCount(word) += 1)
        log.info(s"Total count: ${upload_eqpid.length}")
        var details = ""
        wordCount.foreach(word => details = details + s"${word._1}:${word._2},")
        log.info(s"Details count: ${details}")
        upload_eqpid.clear()
      }
      flush_work = context.system.scheduler.scheduleOnce(60 seconds, self, WRITE_LOG())
    case _ =>
  }
}
