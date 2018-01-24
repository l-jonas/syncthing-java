/* 
 * Copyright (C) 2016 Davide Imbriaco
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.core.security

import com.google.common.cache.CacheBuilder
import com.google.common.hash.Hashing
import com.google.common.io.BaseEncoding
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.interfaces.RelayConnection
import net.syncthing.java.core.utils.NetworkUtils
import org.apache.commons.lang3.tuple.Pair
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v1CertificateBuilder
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.operator.OperatorCreationException
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.eclipse.jetty.alpn.ALPN
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.math.BigInteger
import java.net.InetSocketAddress
import java.net.Socket
import java.security.*
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.*
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLSocket
import javax.security.auth.x500.X500Principal

class KeystoreHandler private constructor(private val keyStore: KeyStore) {

    private val logger = LoggerFactory.getLogger(javaClass)

    class CryptoException internal constructor(t: Throwable) : GeneralSecurityException(t)

    private val socketFactory = TLSSocketFactory(keyStore)

    private fun exportKeystoreToData(): ByteArray {
        val out = ByteArrayOutputStream()
        try {
            keyStore.store(out, JKS_PASSWORD.toCharArray())
        } catch (ex: KeyStoreException) {
            throw RuntimeException(ex)
        } catch (ex: IOException) {
            throw RuntimeException(ex)
        } catch (ex: NoSuchAlgorithmException) {
            throw RuntimeException(ex)
        } catch (ex: CertificateException) {
            throw RuntimeException(ex)
        }
        return out.toByteArray()
    }

    @Throws(CryptoException::class, IOException::class)
    private fun wrapSocket(socket: Socket, isServerSocket: Boolean, vararg protocols: String): Socket {
        try {
            logger.debug("wrapping plain socket, server mode = {}", isServerSocket)
            val sslSocket = socketFactory.createSocket(socket, null, socket.port, true) as SSLSocket
            if (isServerSocket) {
                sslSocket.useClientMode = false
            }
            enableALPN(sslSocket, *protocols)
            return sslSocket
        } catch (e: KeyManagementException) {
            throw CryptoException(e)
        } catch (e: NoSuchAlgorithmException) {
            throw CryptoException(e)
        } catch (e: KeyStoreException) {
            throw CryptoException(e)
        } catch (e: UnrecoverableKeyException) {
            throw CryptoException(e)
        }

    }

    @Throws(CryptoException::class, IOException::class)
    fun createSocket(relaySocketAddress: InetSocketAddress, vararg protocols: String): Socket {
        try {
            val socket = socketFactory.createSocket() as SSLSocket
            socket.connect(relaySocketAddress, SOCKET_TIMEOUT)
            enableALPN(socket, *protocols)
            return socket
        } catch (e: KeyManagementException) {
            throw CryptoException(e)
        } catch (e: NoSuchAlgorithmException) {
            throw CryptoException(e)
        } catch (e: KeyStoreException) {
            throw CryptoException(e)
        } catch (e: UnrecoverableKeyException) {
            throw CryptoException(e)
        }

    }

    private fun enableALPN(socket: SSLSocket, vararg protocols: String) {
        try {
            Class.forName("org.eclipse.jetty.alpn.ALPN")
            ALPN.put(socket, object : ALPN.ClientProvider {

                override fun protocols(): List<String> {
                    return Arrays.asList(*protocols)
                }

                override fun unsupported() {
                    ALPN.remove(socket)
                }

                override fun selected(protocol: String) {
                    ALPN.remove(socket)
                    logger.debug("ALPN select protocol = {}", protocol)
                }
            })
        } catch (cne: ClassNotFoundException) {
            logger.warn("ALPN not available, org.eclipse.jetty.alpn.ALPN not found! ( requires java -Xbootclasspath/p:path/to/alpn-boot.jar )")
        } catch (cne: NoClassDefFoundError) {
            logger.warn("ALPN not available, org.eclipse.jetty.alpn.ALPN not found! ( requires java -Xbootclasspath/p:path/to/alpn-boot.jar )")
        }

    }

    @Throws(SSLPeerUnverifiedException::class, CertificateException::class)
    fun checkSocketCerificate(socket: SSLSocket, deviceId: String) {
        val session = socket.session
        val certs = Arrays.asList(*session.peerCertificates)
        val certificateFactory = CertificateFactory.getInstance("X.509")
        val certPath = certificateFactory.generateCertPath(certs)
        val certificate = certPath.certificates[0]
        NetworkUtils.assertProtocol(certificate is X509Certificate)
        val derData = certificate.encoded
        val deviceIdFromCertificate = derDataToDeviceIdString(derData)
        logger.trace("remote pem certificate =\n{}", derToPem(derData))
        NetworkUtils.assertProtocol(deviceIdFromCertificate == deviceId, {"device id mismatch! expected = $deviceId, got = $deviceIdFromCertificate"})
        logger.debug("remote ssl certificate match deviceId = {}", deviceId)
    }

    @Throws(CryptoException::class, IOException::class)
    fun wrapSocket(relayConnection: RelayConnection, vararg protocols: String): Socket {
        return wrapSocket(relayConnection.getSocket(), relayConnection.isServerSocket(), *protocols)
    }

    class Loader {

        private val logger = LoggerFactory.getLogger(javaClass)

        fun loadAndStore(configuration: ConfigurationService): KeystoreHandler {
            synchronized(keystoreHandlersCacheByHash) {
                var isNew = false
                var keystoreData: ByteArray? = configuration.keystore
                if (keystoreData != null) {
                    val keystoreHandlerFromCache = keystoreHandlersCacheByHash.getIfPresent(BaseEncoding.base32().encode(Hashing.sha256().hashBytes(keystoreData).asBytes()))
                    if (keystoreHandlerFromCache != null) {
                        return keystoreHandlerFromCache
                    }
                }
                val keystoreAlgo = configuration.keystoreAlgo?.let { algo ->
                    if (!algo.isBlank()) algo else null
                } ?: {
                    val defaultAlgo = KeyStore.getDefaultType()!!
                    logger.debug("keystore algo set to {}", defaultAlgo)
                    configuration.Editor().setKeystoreAlgo(defaultAlgo)
                    defaultAlgo
                }()
                var keyStore: Pair<KeyStore, String>? = null
                if (keystoreData != null) {
                    try {
                        keyStore = importKeystore(keystoreData, configuration)
                    } catch (ex: CryptoException) {
                        logger.error("error importing keystore", ex)
                    } catch (ex: IOException) {
                        logger.error("error importing keystore", ex)
                    }

                }
                if (keyStore == null) {
                    try {
                        keyStore = generateKeystore(configuration)
                        isNew = true
                    } catch (ex: CryptoException) {
                        throw RuntimeException("error generating keystore", ex)
                    } catch (ex: IOException) {
                        throw RuntimeException("error generating keystore", ex)
                    }

                }
                val keystoreHandler = KeystoreHandler(keyStore.left)
                if (isNew) {
                    keystoreData = keystoreHandler.exportKeystoreToData()
                    configuration.Editor()
                            .setDeviceId(keyStore.right)
                            .setKeystore(keystoreData)
                            .setKeystoreAlgo(keystoreAlgo)
                            .persistLater()
                }
                keystoreHandlersCacheByHash.put(BaseEncoding.base32().encode(Hashing.sha256().hashBytes(keystoreData!!).asBytes()), keystoreHandler)
                logger.info("keystore ready, device id = {}", configuration.deviceId)
                return keystoreHandler
            }
        }

        @Throws(CryptoException::class, IOException::class)
        private fun generateKeystore(configuration: ConfigurationService): Pair<KeyStore, String> {
            try {
                logger.debug("generating key")
                val keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGO)
                keyPairGenerator.initialize(KEY_SIZE)
                val keyPair = keyPairGenerator.genKeyPair()

                val contentSigner = JcaContentSignerBuilder(SIGNATURE_ALGO).setProvider(BC_PROVIDER).build(keyPair.private)

                val startDate = Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000)
                val endDate = Date(System.currentTimeMillis() + 10 * 365 * 24 * 60 * 60 * 1000)

                val certificateBuilder = JcaX509v1CertificateBuilder(X500Principal(CERTIFICATE_CN), BigInteger.ZERO, startDate, endDate, X500Principal(CERTIFICATE_CN), keyPair.public)

                val certificateHolder = certificateBuilder.build(contentSigner)

                val certificateDerData = certificateHolder.encoded
                logger.info("generated cert =\n{}", derToPem(certificateDerData))
                val deviceId = derDataToDeviceIdString(certificateDerData)
                logger.info("device id from cert = {}", deviceId)

                val keyStore = KeyStore.getInstance(configuration.keystoreAlgo)
                keyStore.load(null, null)
                val certChain = arrayOfNulls<Certificate>(1)
                certChain[0] = JcaX509CertificateConverter().setProvider(BC_PROVIDER).getCertificate(certificateHolder)
                keyStore.setKeyEntry("key", keyPair.private, KEY_PASSWORD.toCharArray(), certChain)
                return Pair.of(keyStore, deviceId)
            } catch (e: OperatorCreationException) {
                throw CryptoException(e)
            } catch (e: CertificateException) {
                throw CryptoException(e)
            } catch (e: NoSuchAlgorithmException) {
                throw CryptoException(e)
            } catch (e: KeyStoreException) {
                throw CryptoException(e)
            }

        }

        @Throws(CryptoException::class, IOException::class)
        private fun importKeystore(keystoreData: ByteArray, configuration: ConfigurationService): Pair<KeyStore, String> {
            try {
                val keystoreAlgo = configuration.keystoreAlgo
                val keyStore = KeyStore.getInstance(keystoreAlgo)
                keyStore.load(ByteArrayInputStream(keystoreData), JKS_PASSWORD.toCharArray())
                val alias = keyStore.aliases().nextElement()
                val certificate = keyStore.getCertificate(alias)
                NetworkUtils.assertProtocol(certificate is X509Certificate)
                val derData = certificate.encoded
                val deviceId = derDataToDeviceIdString(derData)
                logger.debug("loaded device id from cert = {}", deviceId)
                return Pair.of(keyStore, deviceId)
            } catch (e: NoSuchAlgorithmException) {
                throw CryptoException(e)
            } catch (e: KeyStoreException) {
                throw CryptoException(e)
            } catch (e: CertificateException) {
                throw CryptoException(e)
            }

        }

        companion object {
            private val keystoreHandlersCacheByHash = CacheBuilder.newBuilder()
                    .maximumSize(10)
                    .build<String, KeystoreHandler>()
        }
    }

    companion object {

        private val JKS_PASSWORD = "password"
        private val KEY_PASSWORD = "password"
        private val KEY_ALGO = "RSA"
        private val SIGNATURE_ALGO = "SHA1withRSA"
        private val CERTIFICATE_CN = "CN=syncthing"
        private val BC_PROVIDER = "BC"
        private val TLS_VERSION = "TLSv1.2"
        private val KEY_SIZE = 3072
        private val SOCKET_TIMEOUT = 2000

        init {
            Security.addProvider(BouncyCastleProvider())
        }

        private fun derToPem(der: ByteArray): String {
            return "-----BEGIN CERTIFICATE-----\n" + BaseEncoding.base64().withSeparator("\n", 76).encode(der) + "\n-----END CERTIFICATE-----"
        }

        fun derDataToDeviceIdString(certificateDerData: ByteArray): String {
            return hashDataToDeviceIdString(Hashing.sha256().hashBytes(certificateDerData).asBytes())
        }

        fun hashDataToDeviceIdString(hashData: ByteArray): String {
            NetworkUtils.assertProtocol(hashData.size == Hashing.sha256().bits() / 8)
            var string = BaseEncoding.base32().encode(hashData).replace("=+$".toRegex(), "")
            string = string.chunked(13).map { part -> part + generateLuhn32Checksum(part) }.joinToString("")
            return string.chunked(7).joinToString("-")
        }

        fun deviceIdStringToHashData(deviceId: String): ByteArray {
            NetworkUtils.assertProtocol(deviceId.matches("^[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}-[A-Z0-9]{7}$".toRegex()), {"device id syntax error for deviceId = $deviceId"})
            val base32data = deviceId.replaceFirst("(.{7})-(.{6}).-(.{7})-(.{6}).-(.{7})-(.{6}).-(.{7})-(.{6}).".toRegex(), "$1$2$3$4$5$6$7$8") + "==="
            val binaryData = BaseEncoding.base32().decode(base32data)
            NetworkUtils.assertProtocol(binaryData.size == Hashing.sha256().bits() / 8)
            return binaryData
        }

        fun validateDeviceId(peer: String) {
            NetworkUtils.assertProtocol(hashDataToDeviceIdString(deviceIdStringToHashData(peer)) == peer)
        }

        // TODO serialize keystore
        private fun generateLuhn32Checksum(string: String): Char {
            val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
            var factor = 1
            var sum = 0
            val n = alphabet.length
            for (character in string.toCharArray()) {
                val index = alphabet.indexOf(character)
                NetworkUtils.assertProtocol(index >= 0)
                var add = factor * index
                factor = if (factor == 2) 1 else 2
                add = add / n + add % n
                sum += add
            }
            val remainder = sum % n
            val check = (n - remainder) % n
            return alphabet[check]
        }

        val BEP = "bep/1.0"
        val RELAY = "bep-relay"
    }

}
