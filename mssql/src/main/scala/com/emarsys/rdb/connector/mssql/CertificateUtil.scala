package com.emarsys.rdb.connector.mssql

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.security.KeyStore
import java.security.cert.{Certificate, CertificateFactory}
import java.util.concurrent.ConcurrentHashMap

import scala.util.Try

object CertificateUtil {

  val certPool: java.util.Map[String, String] = new ConcurrentHashMap[String, String]()

  def createTrustStoreTempFile(certificate: String): Try[String] = {
    Option(certPool.get(certificate)).map(Try(_)).getOrElse {
      Try {
        val cert     = createCertificateFromString(certificate)
        val keyStore = createKeystoreWithCertificate(cert)
        val filePath = createKeystoreTempFile(keyStore)

        val path = s"file:$filePath"
        certPool.put(certificate, path)
        path
      }
    }
  }

  private def createCertificateFromString(certificate: String): Certificate = {
    CertificateFactory
      .getInstance("X.509")
      .generateCertificate(
        new ByteArrayInputStream(certificate.getBytes)
      )
  }

  private def createKeystoreWithCertificate(certificate: Certificate, password: Array[Char] = Array.empty) = {
    val keyStore = KeyStore.getInstance("jks")
    keyStore.load(null, password)
    keyStore.setCertificateEntry("MsSQLCACert", certificate)
    keyStore
  }

  private def createKeystoreTempFile(keyStore: KeyStore, password: Array[Char] = Array.empty) = {
    val temp = File.createTempFile("keystore", ".keystore")
    temp.deleteOnExit()

    val fos = new FileOutputStream(temp)
    keyStore.store(fos, password)
    fos.close()

    temp.getAbsolutePath
  }

}
