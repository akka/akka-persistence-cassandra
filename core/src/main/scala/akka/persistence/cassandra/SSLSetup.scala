/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.io.{ File, FileInputStream }
import java.security.{ KeyStore, SecureRandom }
import javax.net.ssl.{ KeyManagerFactory, SSLContext, TrustManagerFactory, TrustManager, KeyManager }
import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object SSLSetup {
  /**
   * creates a new SSLContext
   */
  def constructContext(
    trustStore: StorePathPasswordConfig,
    keyStore:   Option[StorePathPasswordConfig]): SSLContext = {

    val tmf = loadTrustManagerFactory(
      trustStore.path,
      trustStore.password)

    val trustManagers: Array[TrustManager] = tmf.getTrustManagers

    val keyManagers: Array[KeyManager] = keyStore.map {
      case StorePathPasswordConfig(path, password) =>
        val kmf = loadKeyManagerFactory(path, password)
        kmf.getKeyManagers
    }.getOrElse(Array.empty[KeyManager])

    val ctx = SSLContext.getInstance("SSL")

    ctx.init(
      keyManagers,
      trustManagers,
      new SecureRandom())

    ctx
  }

  def loadKeyStore(
    storePath:     String,
    storePassword: String): KeyStore = {
    val ks = KeyStore.getInstance("JKS")
    val f = new File(storePath)
    if (!f.isFile) throw new IllegalArgumentException(s"JKSs path $storePath not found.")
    val is = new FileInputStream(f)

    try {
      ks.load(is, storePassword.toCharArray)
    } finally is.close()

    ks
  }

  def loadTrustManagerFactory(
    trustStorePath:     String,
    trustStorePassword: String): TrustManagerFactory = {

    val ts = loadKeyStore(trustStorePath, trustStorePassword)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    tmf
  }

  def loadKeyManagerFactory(
    keyStorePath:     String,
    keyStorePassword: String): KeyManagerFactory = {

    val ks = loadKeyStore(keyStorePath, keyStorePassword)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    val keyPassword = keyStorePassword.toCharArray
    kmf.init(ks, keyPassword)
    kmf
  }
}
