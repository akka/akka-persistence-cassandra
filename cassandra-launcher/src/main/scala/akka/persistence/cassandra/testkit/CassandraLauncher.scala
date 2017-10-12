/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.testkit

import java.io._
import java.net.{ InetSocketAddress, Socket, URI }
import java.nio.channels.ServerSocketChannel
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.annotation.varargs
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
 * Starts Cassandra in current JVM. There can only be one Cassandra instance per JVM,
 * but keyspaces can be used for isolation.
 */
object CassandraLauncher {

  class CleanFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause)

  private val ForcedShutdownTimeout = 20.seconds
  // Used in fork mode to wait for Cassandra to start listening
  private val AwaitListenTimeout = 45.seconds
  private val AwaitListenPoll = 100.millis

  /**
   * Default config for testing "test-embedded-cassandra.yaml"
   */
  val DefaultTestConfigResource: String = "test-embedded-cassandra.yaml"

  /**
   * Main method to start Cassandra, see [[#start]].
   * Note that `cassandra-all` jar must be in classpath.
   *
   * `port can be defined with `-DCassandraLauncher.port=4000`,
   *   default is the `randomPort`
   * `clean` can be defined with `-DCassandraLauncher.clean=true`,
   *   default is `false`
   * `directory` can be defined with `-DCassandraLauncher.directory=target/embedded-cassandra`,
   *   default is `target/embedded-cassandra`
   * `configResource` yaml configuration loaded from classpath,
   *   can be defined with `-DCassandraLauncher.configResource=test-embedded-cassandra.yaml`,
   *   default is defined in [[CassandraLauncher#DefaultTestConfigResource]],
   *   i.e. `test-embedded-cassandra.yaml`
   */
  def main(args: Array[String]): Unit = {
    val port: Int =
      if (args.length > 0) args(0).toInt
      else Integer.getInteger("CassandraLauncher.port", 0)
    val clean =
      if (args.length > 1) args(1).toBoolean
      else java.lang.Boolean.getBoolean("CassandraLauncher.clean")
    val dir =
      if (args.length > 2) new File(args(2))
      else new File(System.getProperty("CassandraLauncher.directory", "target/embedded-cassandra"))
    val configResource =
      if (args.length > 3) args(3)
      else System.getProperty("CassandraLauncher.configResource", DefaultTestConfigResource)
    start(dir, configResource, clean, port)
  }

  private var cassandraDaemon: Option[Closeable] = None

  /**
   * The random free port that will be used if `port=0` is
   * specified in the `start` method.
   */
  lazy val randomPort: Int = freePort()

  def freePort(): Int = {
    val serverSocket = ServerSocketChannel.open().socket()
    serverSocket.bind(new InetSocketAddress("127.0.0.1", 0))
    val port = serverSocket.getLocalPort
    serverSocket.close()
    port
  }

  /**
   * Use this to locate classpath elements from the current classpath to add
   * to the classpath of the launched Cassandra.
   *
   * This is particularly useful if you want a custom logging, you can use
   * this to ensure that the directory that your log file is in is on the
   * classpath of the forked Cassandra process, for example:
   *
   * ```
   * CassandraLauncher.start(
   *   cassandraDirectory,
   *   CassandraLauncher.DefaultTestConfigResource,
   *   clean = true,
   *   port = 0,
   *   CassandraLauncher.classpathForResources("logback.xml")
   * )
   * ```
   */
  @varargs
  def classpathForResources(resources: String*): immutable.Seq[String] = {
    resources.map { resource =>
      this.getClass.getClassLoader.getResource(resource) match {
        case null => sys.error("Resource not found: " + resource)
        case fileUrl if fileUrl.getProtocol == "file" =>
          new File(URI.create(fileUrl.toString.stripSuffix(resource))).getCanonicalPath
        case jarUrl if jarUrl.getProtocol == "jar" =>
          new File(URI.create(jarUrl.getPath.takeWhile(_ != '!'))).getCanonicalPath
      }
    }.distinct.to[immutable.Seq]
  }

  /**
   * Start Cassandra
   *
   * @param cassandraDirectory the data directory to use
   * @param configResource yaml configuration loaded from classpath,
   *   default configuration for testing is defined in [[CassandraLauncher#DefaultTestConfigResource]]
   * @param clean if `true` all files in the data directory will be deleted
   *   before starting Cassandra
   * @param port the `native_transport_port` to use, if 0 a random
   *   free port is used, which can be retrieved (before starting)
   *   with [[CassandraLauncher.randomPort]].
   * @throws akka.persistence.cassandra.testkit.CassandraLauncher.CleanFailedException if `clean`
   *   is `true` and removal of the directory fails
   */
  def start(cassandraDirectory: File, configResource: String, clean: Boolean, port: Int): Unit =
    start(cassandraDirectory, configResource, clean, port, Nil)

  /**
   * Start Cassandra
   *
   * @param cassandraDirectory the data directory to use
   * @param configResource yaml configuration loaded from classpath,
   *   default configuration for testing is defined in [[CassandraLauncher#DefaultTestConfigResource]]
   * @param clean if `true` all files in the data directory will be deleted
   *   before starting Cassandra
   * @param port the `native_transport_port` to use, if 0 a random
   *   free port is used, which can be retrieved (before starting)
   *   with [[CassandraLauncher.randomPort]].
   * @param classpath Any additional jars/directories to add to the classpath. Use
   *                  [[CassandraLauncher#classpathForResources]] to assist in calculating this.
   * @throws akka.persistence.cassandra.testkit.CassandraLauncher.CleanFailedException if `clean`
   *   is `true` and removal of the directory fails
   */
  def start(cassandraDirectory: File, configResource: String, clean: Boolean, port: Int, classpath: immutable.Seq[String]): Unit =
    start(cassandraDirectory, configResource, clean, port, classpath, None)

  /**
   * Start Cassandra
   *
   * @param cassandraDirectory the data directory to use
   * @param configResource yaml configuration loaded from classpath,
   *   default configuration for testing is defined in [[CassandraLauncher#DefaultTestConfigResource]]
   * @param clean if `true` all files in the data directory will be deleted
   *   before starting Cassandra
   * @param port the `native_transport_port` to use, if 0 a random
   *   free port is used, which can be retrieved (before starting)
   *   with [[CassandraLauncher.randomPort]].
   * @param classpath Any additional jars/directories to add to the classpath. Use
   *                  [[CassandraLauncher#classpathForResources]] to assist in calculating this.
   * @param host the host to bind the embeded Cassandra to. If None, then 127.0.0.1 is used.
   * @throws akka.persistence.cassandra.testkit.CassandraLauncher.CleanFailedException if `clean`
   *   is `true` and removal of the directory fails
   */
  def start(cassandraDirectory: File, configResource: String, clean: Boolean, port: Int, classpath: immutable.Seq[String], host: Option[String]): Unit = this.synchronized {
    if (cassandraDaemon.isEmpty) {
      prepareCassandraDirectory(cassandraDirectory, clean)

      val realHost = host.getOrElse("127.0.0.1")
      val realPort = if (port == 0) randomPort else port
      val storagePort = freePort()

      // http://wiki.apache.org/cassandra/StorageConfiguration
      val conf = readResource(configResource)
      val amendedConf = conf
        .replace("$PORT", realPort.toString)
        .replace("$STORAGE_PORT", storagePort.toString)
        .replace("$DIR", cassandraDirectory.getAbsolutePath)
        .replace("$HOST", realHost)
      val configFile = new File(cassandraDirectory, configResource)
      writeToFile(configFile, amendedConf)

      // Extract the cassandra bundle to the directory
      val cassandraBundleFile = new File(cassandraDirectory, "cassandra-bundle.jar")
      if (!cassandraBundleFile.exists()) {
        val is = this.getClass.getClassLoader.getResourceAsStream("akka/persistence/cassandra/launcher/cassandra-bundle.jar")
        try {
          Files.copy(is, cassandraBundleFile.toPath)
        } finally {
          if (is != null) is.close()
        }
      }

      startForked(configFile, cassandraBundleFile, classpath, realHost, realPort)
    }
  }

  private def prepareCassandraDirectory(cassandraDirectory: File, clean: Boolean): Unit = {
    if (clean) {
      try {
        deleteRecursive(cassandraDirectory)
      } catch {
        // deleteRecursive may throw AssertionError
        case e: AssertionError => throw new CleanFailedException(e.getMessage, e)
        case NonFatal(e)       => throw new CleanFailedException(e.getMessage, e)
      }
    }

    if (!cassandraDirectory.exists)
      require(cassandraDirectory.mkdirs(), s"Couldn't create Cassandra directory [$cassandraDirectory]")
  }

  private def startForked(configFile: File, cassandraBundle: File, classpath: immutable.Seq[String], host: String, port: Int): Unit = {
    // Calculate classpath
    val / = File.separator
    val javaBin = s"${System.getProperty("java.home")}${/}bin${/}java"
    val className = "org.apache.cassandra.service.CassandraDaemon"
    val classpathArgument = (classpath :+ cassandraBundle.getAbsolutePath).mkString(File.pathSeparator)

    val builder = new ProcessBuilder(javaBin, "-cp", classpathArgument,
      "-Dcassandra.config=file:" + configFile.getAbsoluteFile, "-Dcassandra-foreground=true", className)
      .inheritIO()

    val process = builder.start()

    val shutdownHook = new Thread {
      override def run(): Unit = {
        process.destroyForcibly()
      }
    }
    Runtime.getRuntime.addShutdownHook(shutdownHook)

    // We wait for Cassandra to start listening before we return, since running in non fork mode will also not
    // return until Cassandra has started listening.
    waitForCassandraToListen(host, port)

    cassandraDaemon = Some(new Closeable {
      override def close(): Unit = {
        process.destroy()
        Runtime.getRuntime.removeShutdownHook(shutdownHook)
        if (process.waitFor(ForcedShutdownTimeout.toMillis, TimeUnit.MILLISECONDS)) {
          val exitStatus = process.exitValue()
          // Java processes killed with SIGTERM may exit with a status of 143
          if (exitStatus != 0 && exitStatus != 143) {
            sys.error(s"Cassandra exited with non zero status: ${process.exitValue()}")
          }
        } else {
          process.destroyForcibly()
          sys.error(s"Cassandra process did not stop within $ForcedShutdownTimeout, killing.")
        }
      }
    })
  }

  /**
   * Stops Cassandra. However, it will not be possible to start Cassandra
   * again in same JVM.
   */
  def stop(): Unit = this.synchronized {
    cassandraDaemon.foreach(_.close())
    cassandraDaemon = None
  }

  private def readResource(resource: String): String = {
    val sb = new StringBuilder
    val is = getClass.getResourceAsStream("/" + resource)
    require(is != null, s"resource [$resource] doesn't exist")
    val reader = new BufferedReader(new InputStreamReader(is))
    try {
      var line = reader.readLine()
      while (line != null) {
        sb.append(line).append('\n')
        line = reader.readLine()
      }
    } finally {
      reader.close()
    }
    sb.toString
  }

  private def writeToFile(file: File, content: String): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"))
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  }

  private def waitForCassandraToListen(host: String, port: Int) = {
    val deadline = AwaitListenTimeout.fromNow
    @annotation.tailrec
    def tryConnect(): Unit = {
      val retry = try {
        new Socket(host, port).close()
        false
      } catch {
        case ioe: IOException if deadline.hasTimeLeft() =>
          Thread.sleep(AwaitListenPoll.toMillis)
          true
        case ioe: IOException =>
          throw new RuntimeException(s"Cassandra did not start within $AwaitListenTimeout", ioe)
      }
      if (retry) tryConnect()
    }
    tryConnect()
  }

  private def deleteRecursive(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursive)
    }
    file.delete()
  }

}

