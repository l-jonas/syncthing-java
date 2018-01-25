package net.syncthing.java.core.security

import java.io.IOException
import java.io.InputStream
import java.net.InetAddress
import java.net.Socket
import java.net.UnknownHostException
import java.security.*
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import javax.net.ssl.*

/**
 * Pre-Lollipop Android devices do not support TLS 1.1 and 1.2 by default, so we have to enable them manually.
 *
 * https://stackoverflow.com/a/38863705
 * http://blog.dev-area.net/2015/08/13/android-4-1-enable-tls-1-1-and-tls-1-2/
 */
class TLSSocketFactory
        @Throws(KeyManagementException::class, NoSuchAlgorithmException::class, KeyStoreException::class, UnrecoverableKeyException::class)
        constructor(keyStore: KeyStore) : SSLSocketFactory() {

    companion object {
        private const val KEY_PASSWORD = "password"
        private const val TLS_VERSION = "TLSv1.2"
    }

    private val internalSSLSocketFactory: SSLSocketFactory

    init {
        val sslContext = SSLContext.getInstance(TLS_VERSION)
        val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        keyManagerFactory.init(keyStore, KEY_PASSWORD.toCharArray())

        sslContext.init(keyManagerFactory.keyManagers, arrayOf(object : X509TrustManager {
            @Throws(CertificateException::class)
            override fun checkClientTrusted(xcs: Array<X509Certificate>, string: String) {}
            @Throws(CertificateException::class)
            override fun checkServerTrusted(xcs: Array<X509Certificate>, string: String) {}
            override fun getAcceptedIssuers() = null
        }), null)
        internalSSLSocketFactory = sslContext.socketFactory
    }

    override fun getDefaultCipherSuites(): Array<String> = internalSSLSocketFactory.defaultCipherSuites

    override fun getSupportedCipherSuites(): Array<String> = internalSSLSocketFactory.supportedCipherSuites

    @Throws(IOException::class)
    override fun createSocket() = enableTLSOnSocket(internalSSLSocketFactory.createSocket())

    @Throws(IOException::class)
    override fun createSocket(s: Socket, host: String?, port: Int, autoClose: Boolean)
            = enableTLSOnSocket(internalSSLSocketFactory.createSocket(s, host, port, autoClose))

    @Throws(IOException::class, UnknownHostException::class)
    override fun createSocket(host: String, port: Int)
            = enableTLSOnSocket(internalSSLSocketFactory.createSocket(host, port))

    @Throws(IOException::class, UnknownHostException::class)
    override fun createSocket(host: String, port: Int, localHost: InetAddress, localPort: Int)
            = enableTLSOnSocket(internalSSLSocketFactory.createSocket(host, port, localHost, localPort))

    @Throws(IOException::class)
    override fun createSocket(host: InetAddress, port: Int)
            = enableTLSOnSocket(internalSSLSocketFactory.createSocket(host, port))

    @Throws(IOException::class)
    override fun createSocket(address: InetAddress, port: Int, localAddress: InetAddress, localPort: Int)
            = enableTLSOnSocket(internalSSLSocketFactory.createSocket(address, port, localAddress, localPort))

    @Throws(IOException::class)
    override fun createSocket(socket: Socket?, inputStream: InputStream?, autoClose: Boolean)
            = enableTLSOnSocket(internalSSLSocketFactory.createSocket(socket, inputStream, autoClose))

    private fun enableTLSOnSocket(socket: Socket): Socket {
        if (socket !is SSLSocket)
            throw InvalidParameterException()
        socket.enabledProtocols = arrayOf("TLSv1.1", "TLSv1.2")
        return socket
    }
}