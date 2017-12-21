package net.syncthing.java.bep

import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.security.KeystoreHandler
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.net.ServerSocket
import java.net.BindException

class ConnectionListener(private val configuration: ConfigurationService,
                         private val connectionListener: (BlockExchangeConnectionHandler) -> Unit) : Closeable {

    private val DefaultListeningPort = 22000
    private val logger = LoggerFactory.getLogger(javaClass)

    private lateinit var serverSocket: ServerSocket

    fun open() {
        createSocket()
        Thread {
            try {
                logger.info("Listening for connections on port ${getListeningPort()}")
                while (!serverSocket.isClosed) {
                    logger.debug("accept 0")
                    val socket = serverSocket.accept()
                    logger.debug("accept 1")
                    val connection = BlockExchangeConnectionHandler(configuration)
                    logger.debug("accept 2")
                    // TODO: need a thread here?
                    connection.accept(socket)
                    logger.debug("accept 3")
                    connectionListener(connection)
                    logger.debug("accept 4")
                }
            } catch (e: Exception) {
                when (e) {
                    is IOException, is KeystoreHandler.CryptoException -> {
                        logger.warn("Failed to listen for incoming connections", e)
                        // handle those above
                    }
                    else -> throw e
                }
            }
        }.start()
    }

    fun getListeningPort() = serverSocket.localPort

    /**
     * Tries to open a server serverSocket on [[DefaultListeningPort]]. If that is taken, tries the
     * next ports in order.
     */
    private fun createSocket() {
        var openedSocket: ServerSocket? = null
        var port = DefaultListeningPort
        while (openedSocket == null) {
            try {
                openedSocket = ServerSocket(port)
            } catch (e: BindException) {
                // Port is taken, try next one.
                port++
            } catch (e: IOException) {
                logger.warn("Failed to listen for connections", e)
                return
            }
        }
        serverSocket = openedSocket
    }

    override fun close() {
        serverSocket.close()
    }
}
