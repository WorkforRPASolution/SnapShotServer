package com.sec.eeg.ars.actor

import java.sql.Timestamp

import akka.actor.Actor
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.{Cluster, Session}
import com.sec.eeg.ars.data._
import com.typesafe.config.Config
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory

case class CheckBuffer()

class BatchWriter(conf:Config, cassandraConnection: Cluster) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val formats = DefaultFormats
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  private val log = LoggerFactory.getLogger(classOf[BatchWriter])

  private val FILETIMESTAMP = """(\d+)_\[(\d+)\]\[(\d+)\]""".r
  var session : Session = _

  val buffer = scala.collection.mutable.ListBuffer.empty[UPLOADFILES]
  var insert_work : akka.actor.Cancellable = null

  private def insertData = {
    if(buffer.length > 0) {
      var query = "BEGIN BATCH "
      buffer.foreach(dt => {
        FILETIMESTAMP.findFirstIn(dt.filename) match {
          case Some(ret) => val FILETIMESTAMP(ts,ex,cy) = ret
            val timestamp = new Timestamp(ts.toLong)
            val date = timestamp.toLocalDateTime.toLocalDate
            val year = date.getYear
            val month = date.getMonthValue
            val day = date.getDayOfMonth
            query = query + s"insert into snapshot2(eqpid,year,month,day,fname,image) values('${dt.hostname}',${year},${month},${day},'${dt.filename}','${dt.image}');"
            query = query + s"insert into snapshotlist2(eqpid,year,month,fname,fname_data) values('${dt.hostname}',${year},${month},'${dt.filename}','${dt.filename}');"

          case _ =>
        }
      })
      query = query + "APPLY BATCH;"

      try {
        session.execute(query)
        log.info(s"${buffer.length} snapshot images is inserted")
      } catch {
        case ex: DriverException =>
          log.warn(s"Cassandra driver exception: ${ex.getMessage}")
        case ex: Throwable =>
          log.warn(s"Exception: ${ex.getMessage}")
      }
    }

    buffer.clear()
  }

  override def receive : Receive = {
    case msg : String =>
      val data = parse(msg)
      val conv = data.extract[UPLOADFILES]
      sender() ! HTTPRETURN("Success", "OK")

      buffer.+=(conv)
      if (buffer.length >= ServiceConfig.BatchWriteCount) {
        if (insert_work != null)
          insert_work.cancel()
        insertData
      }
      insert_work = context.system.scheduler.scheduleOnce(ServiceConfig.BatchWriteDuration, self, CheckBuffer())
    case CheckBuffer() =>
      insertData
      insert_work = context.system.scheduler.scheduleOnce(ServiceConfig.BatchWriteDuration, self, CheckBuffer())
    case _ =>
  }

  override def preStart() : Unit = {
    session = cassandraConnection.connect("ars")
    insert_work = context.system.scheduler.scheduleOnce(ServiceConfig.BatchWriteDuration, self, CheckBuffer())
    log.info("BatchWriter preStart")
  }

  override def postStop() : Unit = {
    try {
      if (session != null) {
        session.close()
      }
    } catch {
      case _ =>
    }

    log.info(s"BatchWriter postStop")
  }
}
