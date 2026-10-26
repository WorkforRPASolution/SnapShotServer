package com.sec.eeg.ars.actor

import java.sql.Timestamp

import akka.actor.Actor
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.{Cluster, Session}
import com.sec.eeg.ars.data._
import com.typesafe.config.Config
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory

class ReadWorker(conf: Config, cassandraConnection: Cluster) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val formats = DefaultFormats
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  private val log = LoggerFactory.getLogger(classOf[ReadWorker])

  private val FILETIMESTAMP = """(\d+)_\[(\d+)\]\[(\d+)\]""".r
  var session : Session = _

  override def receive : Receive = {
    case msg : (String, String) =>

      msg._1 match {
        case "DIR" =>
          val data = parse(msg._2)
          val conv = data.extract[DIRSNAPSHOT]

          try {
            val filelist = scala.collection.mutable.MutableList.empty[String]
            log.info(s"DIR_SnapShot(${conv.hostname},${conv.year},${conv.month}) received")
            val query2 = s"select fname from snapshotlist where eqpid='${conv.hostname}' and year=${conv.year} and month=${conv.month};"
            val result2 = session.execute(query2)
            while (result2.iterator().hasNext) {
              val data = result2.iterator().next()
              val fname = data.getString("fname")
              log.debug(s"fname:${fname}")
              filelist += fname
            }
            sender() ! DIRRETURN("Success", "OK", filelist.toList)
          } catch {
            case ex: DriverException =>
              log.warn(s"Cassandra driver exception: ${ex.getMessage}")
              sender() ! DIRRETURN("failed", ex.getMessage, List.empty[String])
            case ex: NullPointerException =>
              log.warn(s"NullPointerException: ${ex.getMessage}")
              sender() ! DIRRETURN("failed", ex.getMessage, List.empty[String])
            case ex: Throwable =>
              log.warn(s"Exception: ${ex.getMessage}")
              sender() ! DIRRETURN("failed", ex.getMessage, List.empty[String])
          }

        case "GETFILE" =>
          val data = parse(msg._2)
          val conv = data.extract[GETFILE]

          FILETIMESTAMP.findFirstIn(conv.filename) match {
            case Some(ret) => val FILETIMESTAMP(ts,ex,cy) = ret
              try{
                var query = ""
                val target_ts = ts.toLong
                val timestamp = new Timestamp(target_ts)
                val date = timestamp.toLocalDateTime.toLocalDate
                val year = date.getYear
                val month = date.getMonthValue
                val day = date.getDayOfMonth
                query = s"select image from snapshot where eqpid='${conv.hostname}' and year=${year} and month=${month} and day=${day} and fname='${conv.filename}';"

                val result = session.execute(query)
                val data = result.one()
                if (data != null) {
                  sender() ! IMGRETURN("Success", "OK", data.getString("image"))
                } else {
                  sender() ! IMGRETURN("failed", "file not found", "")
                }
              } catch {
                case ex: DriverException =>
                  log.warn(s"Cassandra driver exception: ${ex.getMessage}")
                  sender() ! IMGRETURN("failed", ex.getMessage, "")
                case ex: NullPointerException =>
                  log.warn(s"NullPointerException: ${ex.getMessage}")
                  sender() ! IMGRETURN("failed", ex.getMessage, "")
                case ex: Throwable =>
                  log.warn(s"Exception: ${ex.getMessage}")
                  sender() ! IMGRETURN("failed", ex.getMessage, "")
              }
            case None =>
          }
        case _ =>
      }
    case _ =>
  }

  override def preStart() : Unit = {
    session = cassandraConnection.connect("ars")
    log.info("ReadWorker preStart")
  }

  override def postStop() : Unit = {
    try {
      if (session != null) {
        session.close()
      }
    } catch {
      case _ =>
    }

    log.info(s"ReadWorker postStop")
  }
}
