// Copyright (c) 2015, Thilo Planz.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the Apache License, Version 2.0
// as published by the Apache Software Foundation (the "License").
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// You should have received a copy of the License along with this program.
// If not, see <http://www.apache.org/licenses/LICENSE-2.0>.


import java.net.{Socket, InetAddress}
import java.security.cert.{CertificateException, X509Certificate}
import javax.net.ssl.{SSLSocketFactory, X509TrustManager, SSLContext}

import org.apache.commons.codec.digest.DigestUtils

import scala.collection.mutable.ArrayBuffer


/**
 *
 * Helper class to set up the SSLContext so that connections are only allowed
 * to servers with a well-known certificate.
 *
 * The certificate is identified by the SHA-256 hash of its SSL certificate.
 *
 * <code>
 * new SSLKeyPinning( sha256HexDigestOfServerCert ).setDefaultSSLContext()
 * // continue to use HTTPS clients normally
 * </code>
 *
 */


class SSLKeyPinning(trustedDigest: String = null, socketFactory : SSLSocketFactory = SSLContext.getDefault().getSocketFactory) extends SSLSocketFactory with X509TrustManager  {

  private val trusted : ArrayBuffer[String] = if (trustedDigest == null) ArrayBuffer() else ArrayBuffer(trustedDigest)


  /**
   * add another pinned certificate
   */
  def addTrusted(digest: String): Unit ={
    trusted += digest
  }


  /**
   * enable the key pinning by setting the default SSLContext.
   *
   * This is a system-wide setting.
   * There may be less intrusive ways if you control over how
   * your HTTPS client is configured.
   */
  def setDefaultSSLContext() {
    val sslContext = SSLContext.getInstance("SSL")
    sslContext.init(null, Array(this), null)
    SSLContext.setDefault(sslContext)
  }




  // implementation of X509TrustManager

  override def checkClientTrusted(chain: Array[X509Certificate], authType: String)=  {
    throw new CertificateException("Key pinning is only implemented for server certificates")
  }

  override def checkServerTrusted(chain: Array[X509Certificate], authType: String) {
    val serverCert = chain(0)
    val digest = DigestUtils.sha256Hex(serverCert.getEncoded)
    for(t <- trusted){
       if (t.equals(digest)) return;
    }
    // TODO: public key pinning (instead of certificate pinning)
    throw new CertificateException("Server certificate for "+serverCert.getSubjectX500Principal.getName+" has not been pinned. Found sha256 digest of "+digest)
  }

  override def getAcceptedIssuers() : Array[X509Certificate] = {
    return Array()
  }

  // implementation of SSLSocketFactory
  // (just delegates to the default socketFactory, we just supply a TrustManager)

  override def getDefaultCipherSuites: Array[String] = socketFactory.getDefaultCipherSuites

  override def getSupportedCipherSuites: Array[String] = socketFactory.getDefaultCipherSuites

  override def createSocket(socket: Socket, s: String, i: Int, b: Boolean): Socket = socketFactory.createSocket(socket, s, i, b)

  override def createSocket(s: String, i: Int): Socket = socketFactory.createSocket(s,i)

  override def createSocket(s: String, i: Int, inetAddress: InetAddress, i1: Int): Socket = socketFactory.createSocket(s, i, inetAddress, i1)

  override def createSocket(inetAddress: InetAddress, i: Int): Socket = socketFactory.createSocket(inetAddress, i)

  override def createSocket(inetAddress: InetAddress, i: Int, inetAddress1: InetAddress, i1: Int): Socket = socketFactory.createSocket(inetAddress, i, inetAddress1, i1)



}

