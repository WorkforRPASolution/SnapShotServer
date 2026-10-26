package com.sec.eeg.ars.data

import java.io.FileReader
import java.util.Properties

import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import org.slf4j.{LoggerFactory, MDC}

import scala.concurrent.duration.FiniteDuration

object ServiceConfig {
  private val logger = LoggerFactory.getLogger(ServiceConfig.toString)
  val AkkaSystem = "EARSSystem"
  val serviceName = "ARSSnapShotServer"
  var CassandraAddress : String = ""
  var CassandraAuthenticationEnable = false
  val ZookeeperNodePath : String = "/EARS/ARSSnapShotServer"
  var ZookeeperQuorum : String = ""
  var MyServiceAddress : String = ""
  var RedisSentinelConnection : String = _
  var ARSSnapShotServerRedisKey : String = "SnapshotServerInfo"
  var WriterWorkerCount = 10
  var ReadWorkerCount = 10
  var FormatTransformerRouteCount = 5
  var Version = "unknown"
  var ChangePoint = 0L
  val root_dir = System.getProperty("EEG_BASE")
  val redisFieldName = "ARSSnapShotServerChangePoint"
  var BatchWriteCount = 20
  var BatchWriteDuration : FiniteDuration = _

  var MyAddress = ""
  var HttpPort : Int = 8080

  var CassandraUserName = ""
  var CassandraPassword = ""

  def load : Boolean = {
    val parser = new JSONParser
    val confjson = parser.parse(new FileReader(root_dir + s"/conf/${serviceName}/${serviceName}.json")).asInstanceOf[JSONObject]

    try {
      CassandraAddress = confjson.get("CassandraAddress").asInstanceOf[String]
    } catch {
      case ex: Throwable =>
        if (CassandraAddress == null || CassandraAddress.trim == "") {
          System.exit(7)
        }
    }

    try {
      ZookeeperQuorum = confjson.get("ZookeeperQuorum").asInstanceOf[String]
    } catch {
      case ex: Throwable =>
        if (ZookeeperQuorum == null || ZookeeperQuorum.trim == "") {
          System.exit(4)
        }
    }

    val _MyServiceAddress = System.getenv("HOST_IP")
    if (_MyServiceAddress == null || _MyServiceAddress.trim == "") {
      System.exit(5)
    }
    val _HttpPort = System.getenv("HTTP_PORT")
    if (_HttpPort == null || _HttpPort.trim == "") {
      System.exit(5)
    }
    HttpPort = _HttpPort.toInt
    MyServiceAddress = s"${_MyServiceAddress}:${HttpPort}"

    try {
      WriterWorkerCount = confjson.get("WriterWorkerCount").asInstanceOf[Long].toInt
      if (WriterWorkerCount == 0) WriterWorkerCount = 10
    }
    catch {
      case _: Throwable => WriterWorkerCount = 10
    }

    try {
      ReadWorkerCount = confjson.get("ReadWorkerCount").asInstanceOf[Long].toInt
      if (ReadWorkerCount == 0) ReadWorkerCount = 10
    }
    catch {
      case _: Throwable => ReadWorkerCount = 10
    }

    try {
      FormatTransformerRouteCount = confjson.get("FormatTransformerRouteCount").asInstanceOf[Long].toInt
      if (FormatTransformerRouteCount == 0) FormatTransformerRouteCount = 5
    }
    catch {
      case _: Throwable => FormatTransformerRouteCount = 5
    }

    try {
      CassandraAuthenticationEnable = confjson.get("CassandraAuthenticationEnable").asInstanceOf[Boolean]
    } catch {
      case _: Throwable =>
    }

    try {
      BatchWriteCount = confjson.get("BatchWriteCount").asInstanceOf[Long].toInt
      if (BatchWriteCount == 0) BatchWriteCount = 1
    }
    catch {
      case _: Throwable => BatchWriteCount = 1
    }

    var BatchWriteDurationSeconds = 0
    try {
      BatchWriteDurationSeconds = confjson.get("BatchWriteDurationSeconds").asInstanceOf[Long].toInt
      if (BatchWriteDurationSeconds <= 0) BatchWriteDurationSeconds = 1
    }
    catch {
      case _: Throwable => BatchWriteDurationSeconds = 1
    }
    BatchWriteDuration = FiniteDuration(BatchWriteDurationSeconds, "seconds")

    CassandraUserName = System.getenv("Username").trim

    val _cassandraUserName = System.getenv("Username").trim
    if (_cassandraUserName == null || _cassandraUserName.trim == "") {
      logger.warn(s"cassandra username is not exist")
      System.exit(5)
    }
    CassandraUserName = _cassandraUserName

    val _cassandraPassword = System.getenv("Password").trim
    if (_cassandraPassword == null || _cassandraPassword.trim == "") {
      logger.warn(s"cassandra password is not exist")
      System.exit(5)
    }
    CassandraPassword = _cassandraPassword

    logger.info(s"cassandra username: ${CassandraUserName}")
    logger.info(s"cassandra password: ${CassandraPassword}")

    getVersion

    return true
  }

  def getVersion = {
    val is = this.getClass.getClassLoader.getResourceAsStream("META-INF/maven/com.sec.eeg.ars/ARSSnapShotServer/pom.properties")

    if (is != null) {
      val p = new Properties()
      p.load(is)
      Version = p.getProperty("version", "")
    }
    if (Version == null || Version == "") {
      val aPackage = getClass.getPackage
      if (aPackage != null) {
        Version = aPackage.getImplementationVersion
        if (Version == null || Version == "") {
          Version = aPackage.getSpecificationVersion
        }
      }
    }
    if (Version == null)
      Version = "unknown"

    logger.info(s"program release version: ${Version}")
  }
}
