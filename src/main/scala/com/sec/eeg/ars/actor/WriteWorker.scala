package com.sec.eeg.ars.actor

import java.sql.Timestamp

import akka.actor.Actor
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.{Cluster, Session}
import com.sec.eeg.ars.data._
import com.typesafe.config.Config
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory

class WriteWorker(conf: Config, cassandraConnection: Cluster) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val formats = DefaultFormats
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  private val log = LoggerFactory.getLogger(classOf[WriteWorker])

  private val FILETIMESTAMP = """(\d+)_\[(\d+)\]\[(\d+)\]""".r
  var session : Session = _

  override def receive : Receive = {
    case msg : String =>
      val data = parse(msg)
      val conv = data.extract[UPLOADFILES]

      sender() ! HTTPRETURN("Success", "OK")
      log.info(s"received UPLOAD: ${conv.hostname}, ${conv.filename}")

      FILETIMESTAMP.findFirstIn(conv.filename) match {
        case Some(ret) => val FILETIMESTAMP(ts,cx,cy) = ret
          try {
            val timestamp = new Timestamp(ts.toLong)
            val date = timestamp.toLocalDateTime.toLocalDate
            val year = date.getYear
            val month = date.getMonthValue
            val day = date.getDayOfMonth
            val query = s"insert into snapshot(eqpid,year,month,day,fname,image) values('${conv.hostname}',${year},${month},${day},'${conv.filename}','${conv.image}');"
            session.executeAsync(query)
            val query_list = s"insert into snapshotlist(eqpid,year,month,fname,fname_data) values('${conv.hostname}',${year},${month},'${conv.filename}','${conv.filename}');"
            session.executeAsync(query_list)
          } catch {
            case ex: DriverException =>
              log.warn(s"Cassandra driver exception: ${ex.getMessage}")
              throw ex
            case ex: Throwable =>
              log.warn(s"Exception: ${ex.getMessage}")
          }
        case None =>
      }
    case _ =>
  }

  override def preStart() : Unit = {
    session = cassandraConnection.connect("ars")
    log.info("WriteWorker preStart")
  }

  override def postStop() : Unit = {
    try {
      if (session != null) {
        session.close()
      }
    } catch {
      case _ =>
    }

    log.info(s"WriteWorker postStop")
  }
}
