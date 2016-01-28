// Copyright (c) 2015/2016, Thilo Planz.
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


import java.net.Socket
import java.security.cert.{CertificateException, X509Certificate}
import javax.net.ssl._

import org.apache.commons.codec.digest.DigestUtils


/**
 *
 * Helper class to set up the SSLContext so that connections are only allowed
 * to servers with a well-known certificate.
 *
 * The certificate is identified by the SHA-256 hash of its SSL certificate.
 *
 * <code>
 * SSLKeyPinning( sha256HexDigestOfServerCert ).setDefaultSSLContext()
 * // continue to use HTTPS clients normally
 * </code>
 *
 */


class SSLKeyPinning private (trustedDigests: Seq[String])   {


  /**
   * create an SSLContext that implements the key pinning
   */
  def makeSSLContext(skipX509ExtendedTrustVerification: Boolean = false) = {
    val sslContext = SSLContext.getInstance("SSL")
    sslContext.init(null, Array(if (skipX509ExtendedTrustVerification) new ExtendedTrustManager else new TrustManager {}), null)
    sslContext
  }


  /**
   * enable the key pinning by setting the default SSLContext.
   *
   * This is a system-wide setting.
   * There may be less intrusive ways if you control how
   * your HTTPS client is configured.
   */
  def setDefaultSSLContext(skipX509ExtendedTrustVerification: Boolean = false) = SSLContext.setDefault(makeSSLContext(skipX509ExtendedTrustVerification))


  // implementation of X509TrustManager. Using this results in the system providing the default extended trust verification code
  private trait TrustManager extends X509TrustManager {
    override def checkClientTrusted(chain: Array[X509Certificate], authType: String) = {
      throw new CertificateException("Key pinning is only implemented for server certificates")
    }

    override def checkServerTrusted(chain: Array[X509Certificate], authType: String) {
      val serverCert = chain(0)
      val digest = DigestUtils.sha256Hex(serverCert.getEncoded)
      if (trustedDigests.contains(digest)) return;
      // TODO: public key pinning (instead of certificate pinning)
      throw new CertificateException("Server certificate for " + serverCert.getSubjectX500Principal.getName + " has not been pinned. Found sha256 digest of " + digest)
    }

    override def getAcceptedIssuers(): Array[X509Certificate] = {
      return Array()
    }
  }


  // implementation of X509ExtendedTrustManager (that just skips the extended trust verification, such as hostname checking)
  private class ExtendedTrustManager extends X509ExtendedTrustManager with TrustManager{

    // we don't do the additional checks at all
    override def checkClientTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit = checkClientTrusted(chain, authType)

    override def checkClientTrusted(chain: Array[X509Certificate], authType: String, sslEngine: SSLEngine): Unit  = checkClientTrusted(chain, authType)

    override def checkServerTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit  = checkServerTrusted(chain, authType)

    override def checkServerTrusted(chain: Array[X509Certificate], authType: String, sslEngine: SSLEngine): Unit  = checkServerTrusted(chain, authType)
  }


}

object SSLKeyPinning {
  def apply(trustedDigests: Iterable[String]) = new SSLKeyPinning(trustedDigests.toSeq)
  def apply(trustedDigests: String*) = new SSLKeyPinning(trustedDigests.toSeq)
}


