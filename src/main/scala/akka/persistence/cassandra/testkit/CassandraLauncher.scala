package akka.persistence.cassandra.testkit

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel
import org.apache.cassandra.io.util.FileUtils
import org.apache.cassandra.service.CassandraDaemon

/**
 * Starts Cassandra in current JVM. There can only be one Cassandra instance per JVM,
 * but keyspaces can be used for isolation.
 */
object CassandraLauncher {

  /**
   * Default config for testing "test-embedded-cassandra.yaml"
   */
  val DefaultTestConfigResource: String = "test-embedded-cassandra.yaml"

  /**
   * Main method to start Cassandra, see [[#start]].
   * Note that `cassandra-all` jar must be in classpath.
   *
   * `port can be defined with `-DCassandraLauncher.port=4000`
   * `clean` can be defined with `-DCassandraLauncher.clean=true`
   * `directory` can be defined with `-DCassandraLauncher.directory=target/embedded-cassandra`
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
    CassandraLauncher.start(dir, CassandraLauncher.DefaultTestConfigResource, clean, port)
  }

  private var cassandraDaemon: Option[CassandraDaemon] = None

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
   */
  def start(cassandraDirectory: File, configResource: String, clean: Boolean, port: Int): Unit = this.synchronized {
    if (cassandraDaemon.isEmpty) {

      if (clean)
        FileUtils.deleteRecursive(cassandraDirectory);

      if (!cassandraDirectory.exists)
        require(cassandraDirectory.mkdirs(), s"Couldn't create Cassandra directory [$cassandraDirectory]")

      val realPort = if (port == 0) randomPort else port
      val storagePort = freePort()

      // http://wiki.apache.org/cassandra/StorageConfiguration
      val conf = readResource(configResource)
      val amendedConf = conf
        .replace("$PORT", realPort.toString)
        .replace("$STORAGE_PORT", storagePort.toString)
        .replace("$DIR", cassandraDirectory.getAbsolutePath)
      val configFile = new File(cassandraDirectory, configResource)
      writeToFile(configFile, amendedConf)

      System.setProperty("cassandra.config", "file:" + configFile.getAbsolutePath)
      System.setProperty("cassandra-foreground", "true")

      // runManaged = true to avoid System.exit
      val daemon = new CassandraDaemon(true)
      daemon.activate()
      cassandraDaemon = Some(daemon)
    }
  }

  /**
   * Stops Cassandra. However, it will not be possible to start Cassandra
   * again in same JVM.
   */
  def stop(): Unit = this.synchronized {
    cassandraDaemon.foreach(_.deactivate())
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
      writer.write(content);
    } finally {
      writer.close()
    }
  }

}
