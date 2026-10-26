package com.sec.eeg.daemon

import java.io.File

import com.sec.eeg.ars.actor.Master
import com.sec.eeg.ars.data.ServiceConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.daemon.{Daemon, DaemonContext}
import org.slf4j.LoggerFactory

class Application extends Daemon {
  private val logger = LoggerFactory.getLogger(classOf[Application])
  var conf: Config = _

  override def init(daemonContext: DaemonContext): Unit = {
    logger.info("ARSHBaseCamelServer service init")
    val root_dir = System.getProperty("EEG_BASE")
    val akkaApplicationConf = new File(ServiceConfig.root_dir + s"/conf/${ServiceConfig.serviceName}/application.conf")
    conf = ConfigFactory.parseFile(akkaApplicationConf)
  }

  override def stop(): Unit = {
    try {
      logger.info("ARSHBaseCamelServer service will be stopped")
      Master.StopForTermination()
      logger.info("ARSHBaseCamelServer stopped")
    }
    catch {
      case ex : Exception => logger.error(s"When service stop, exception occurs: ${ex}")
    }
  }

  override def destroy(): Unit = {
    logger.info("ARSHBaseCamelServer service destroyed")
  }

  override def start(): Unit = {
    try {
      logger.info("ARSHBaseCamelServer service will be started")
      Master.MakeActor()
      logger.info("ARSHBaseCamelServer started")
    }
    catch {
      case ex : Exception => logger.error(s"When service start, exception occurs: ${ex}")
    }
  }
}