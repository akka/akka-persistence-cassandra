package akka.persistence.cassandra

import java.io.{File, FileInputStream, InputStream}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

private [cassandra] object SSLSetup {
  /**
   * creates a new SSLContext
   */
  def constructContext(
    trustStorePath:String,
    trustStorePW:String,
    keyStorePath:String,
    keyStorePW:String):SSLContext = {

    val tmf = loadTrustManagerFactory(trustStorePath, trustStorePW)
    val kmf = loadKeyManagerFactory(keyStorePath, keyStorePW)

    val ctx = SSLContext.getInstance("SSL")   
    
    ctx.init(
      kmf.getKeyManagers, 
      tmf.getTrustManagers, 
      new SecureRandom())
    
    ctx
  }

  def loadKeyStore(
    storePath:String,
    storePassword:String):KeyStore = {
    val ks = KeyStore.getInstance("JKS")
    val f = new File(storePath)
    if(!f.isFile) throw new IllegalArgumentException(s"JKSs path $storePath not found.")
    val is = new FileInputStream(f)

    try {
      ks.load(is, storePassword.toCharArray)
    } finally (is.close())

    ks
  }

  def loadTrustManagerFactory(
    trustStorePath:String,
    trustStorePassword:String):TrustManagerFactory = {
    
    val ts = loadKeyStore(trustStorePath, trustStorePassword)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    tmf
  }

  def loadKeyManagerFactory(
    keyStorePath:String,
    keyStorePassword:String):KeyManagerFactory = {
    
    val ks = loadKeyStore(keyStorePath, keyStorePassword)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, keyStorePassword.toCharArray)
    kmf
  }
}