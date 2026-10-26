package com.sec.eeg.ars.actor

import java.io.File
import java.net.InetAddress
import java.util.concurrent.TimeUnit

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import akka.pattern.{BackoffOpts, BackoffSupervisor}
import akka.routing.SmallestMailboxPool
import com.datastax.driver.core.policies.{ExponentialReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, ConsistencyLevel, QueryOptions}
import com.sec.eeg.ars.data.ServiceConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.leader.{LeaderSelector}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.CloseableUtils
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object Master {
  final val logger = LoggerFactory.getLogger(Master.toString)
  val serviceName = "ARSHttpFileSystem"
  case class ShutDown()
  var system : ActorSystem = _
  var master : ActorRef = _
  var conf : Config = _
  val stoplock = new Object()

  init

  def init = {
    try {
      val akkaApplicationConf = new File(ServiceConfig.root_dir + s"/conf/${ServiceConfig.serviceName}/application.conf")
      conf = ConfigFactory.parseFile(akkaApplicationConf)
    }
    catch {
      case ex : Exception =>
    }
  }

  def MakeActor(): Unit = {
    if(master == null){
      if(ServiceConfig.load == false)
        throw new Exception("Service Config loading is failed")
      system = ActorSystem.create(ServiceConfig.AkkaSystem, ConfigFactory.load(conf))
      master = system.actorOf(Props[Master], "Master")
    }
  }

  def StopForTermination(): Unit = {
    if(master != null){
      master.tell(ShutDown, ActorRef.noSender)
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      return
    }

    try{
      if(args(0) == "start") {
        MakeActor()
        logger.info(s"$serviceName service started")
      }
      else if(args(0) == "stop") {
        stoplock.synchronized{
          StopForTermination()
          logger.info(s"$serviceName stopped")
        }
      }
    }
    catch {
      case ex: Exception => logger.error(s"program execution failed: ${ex}")
    }
  }
}

class Master extends Actor {
  private val log = LoggerFactory.getLogger(classOf[Master])
  import Master._

  var childsCnt : Int = 0
  var selectorService: LeaderSelector = null
  var zkCli : CuratorFramework = null
  var cassandraConnection: Cluster = null

  private  def initializeCassandra = {
    import scala.collection.JavaConversions._
    val ctpLists = scala.collection.mutable.ListBuffer.empty[InetAddress]
    val address_port = ServiceConfig.CassandraAddress.split("#")
    if(address_port(0) != "" && address_port(1) != ""){
      val contactPoints = address_port(0).split(",")
      contactPoints.foreach(ctp => {
        if(ctp != "")
          ctpLists.+=(InetAddress.getByName(ctp))
      })
      if(contactPoints.length == 0)
        throw new Exception("Invalid Cassandra connection info")

      if(ServiceConfig.CassandraAuthenticationEnable == true){
        cassandraConnection = Cluster.builder.addContactPoints(ctpLists).withCredentials(ServiceConfig.CassandraUserName,ServiceConfig.CassandraPassword).withPort(address_port(1).toInt)
          .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
          .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
          .withReconnectionPolicy(new ExponentialReconnectionPolicy(10, 10000)).build()
      }
      else {
        cassandraConnection = Cluster.builder.addContactPoints(ctpLists).withPort(address_port(1).toInt)
          .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
          .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
          .withReconnectionPolicy(new ExponentialReconnectionPolicy(10, 10000)).build()
      }
    }
  }

  private def makeChild() = {
    initializeCassandra

    val WriteWorkerProps = Props.create(classOf[WriteWorker], conf, cassandraConnection)
    val ReadWorkerProps = Props.create(classOf[ReadWorker], conf, cassandraConnection)
    val BatchWriterProps = Props.create(classOf[BatchWriter], conf, cassandraConnection)

    val BatchWriterSupervisor = BackoffSupervisor.props(
      BackoffOpts.onFailure(BatchWriterProps, "BatchWriter", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        }
      )
    )

    val WriteWorkerSupervisor = BackoffSupervisor.props(
      BackoffOpts.onFailure(WriteWorkerProps, "WriteWorker", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        }
      )
    )

    val ReadWorkerSupervisor = BackoffSupervisor.props(
      BackoffOpts.onFailure(ReadWorkerProps, "ReadWorker", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        }
      )
    )

    val WriterRouter = createChildActor(context.actorOf(SmallestMailboxPool(ServiceConfig.WriterWorkerCount).props(WriteWorkerSupervisor), "WriteWorker"))
    val ReadRouter = createChildActor(context.actorOf(SmallestMailboxPool(ServiceConfig.ReadWorkerCount).props(ReadWorkerSupervisor), "ReadWorker"))

    val SnapshotEndpointProps = Props.create(classOf[HttpEndPoint])
    val SnapshotEndpointSupervisor = BackoffSupervisor.props(
      BackoffOpts.onFailure(SnapshotEndpointProps, "SnapshotEndpoint", Duration.create(3, TimeUnit.SECONDS), Duration.create(30, TimeUnit.SECONDS), 0.2
      ).withSupervisorStrategy(
        OneForOneStrategy() {
          case ex: Throwable =>
            log.info(s"Master supervisorStrategy: ${ex}")
            Restart
        }
      )
    )
    createChildActor(context.actorOf(SnapshotEndpointSupervisor, "SnapshotEndpoint"))
  }

  override def preStart() : Unit = {

    makeChild()

    zkCli = CuratorFrameworkFactory.newClient(ServiceConfig.ZookeeperQuorum, new ExponentialBackoffRetry(1000, 3))
    zkCli.start()
    zkCli.getZookeeperClient.blockUntilConnectedOrTimedOut()
    if(zkCli.getState != CuratorFrameworkState.STARTED){
      throw new Exception("Zk startup timed out")
    }

    if (zkCli.checkExists().creatingParentContainersIfNeeded().forPath(s"${ServiceConfig.ZookeeperNodePath}/daemons") == null)
      zkCli.create().forPath(s"${ServiceConfig.ZookeeperNodePath}/daemons")
    zkCli.create().withMode(CreateMode.EPHEMERAL).forPath(s"${ServiceConfig.ZookeeperNodePath}/daemons/${ServiceConfig.MyServiceAddress}", ServiceConfig.Version.getBytes)

    log.info("Master preStart")
  }

  private def createChildActor(childActor: ActorRef): ActorRef = {
    childsCnt += 1
    context.watch(childActor)
  }

  override def postStop() : Unit = {
    try{
      if(cassandraConnection != null){
        cassandraConnection.close()
      }
    }catch {
      case _ =>
    }
    log.info("Master postStop")
    system.terminate()
  }

  def receive = {
    case ShutDown() =>
      log.info("Shutdown received")

      if(zkCli != null){
        zkCli.delete().forPath(s"${ServiceConfig.ZookeeperNodePath}/daemons/${ServiceConfig.MyServiceAddress}")
        CloseableUtils.closeQuietly(zkCli)
      }

      val cnt = context.children.size
      if(cnt <= 0){
        context stop self
        system.terminate()
      }
      else{
        context.children.foreach(ch => {
          ch ! PoisonPill
        })
        context.become(shuttingDown)
      }
  }

  def shuttingDown: Receive = {
    case Terminated(actor) =>
      log.debug(s"Actor terminated: ${actor}")
      context.unwatch(actor)
      childsCnt -= 1
      log.debug(s"childCnt: ${childsCnt}")
      if(childsCnt <= 0){
        context stop self
      }
  }
}
