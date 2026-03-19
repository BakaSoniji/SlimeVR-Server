package dev.slimevr.osc

import dev.slimevr.oscquery.OSCQueryNode
import dev.slimevr.oscquery.OSCQueryServer
import dev.slimevr.oscquery.OscTransport
import dev.slimevr.oscquery.ServiceInfo
import dev.slimevr.oscquery.fetchHostInfo
import dev.slimevr.oscquery.randomFreePort
import dev.slimevr.protocol.rpc.setup.RPCUtil
import io.eiren.util.logging.LogManager
import java.io.IOException
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.NetworkInterface
import kotlin.concurrent.thread

private const val serviceStartsWith = "VRChat-Client"
private const val queryPath = "/tracking/vrsystem"

/**
 * Handler for OSCQuery for VRChat using our library
 * https://github.com/SlimeVR/oscquery-kt
 */
class VRCOSCQueryHandler(
	private val vrcOscHandler: VRCOSCHandler,
) {
	private val oscQueryServer: OSCQueryServer

	init {
		// Request data
		val localIp = RPCUtil.getLocalIp() ?: throw IllegalStateException("No local IP address found for OSCQuery to bind to")
		val httpPort = randomFreePort()
		oscQueryServer = OSCQueryServer(
			"SlimeVR-Server-$httpPort",
			OscTransport.UDP,
			localIp,
			vrcOscHandler.portIn.toUShort(),
			httpPort,
		)
		oscQueryServer.rootNode.addNode(OSCQueryNode(queryPath))
		oscQueryServer.init()
		LogManager.info("[VRCOSCQueryHandler] SlimeVR OSCQueryServer started at http://$localIp:$httpPort")

		try {
			// Set up browse addresses for all usable interfaces
			val browseAddresses = NetworkInterface.getNetworkInterfaces().asSequence()
				.filter { it.isUp && !it.isLoopback && !it.isVirtual }
				.flatMap { it.inetAddresses.asSequence() }
				.filter { it.isSiteLocalAddress && it is Inet4Address }
				.toList()
			if (browseAddresses.isNotEmpty()) {
				oscQueryServer.setBrowseAddresses(browseAddresses)
				LogManager.info("[VRCOSCQueryHandler] Browsing on ${browseAddresses.joinToString { it.hostAddress }}")
			}

			// Listen for VRChat's OSCQuery service via _oscjson._tcp
			LogManager.info("[VRCOSCQueryHandler] Listening for VRChat OSCQuery (_oscjson._tcp)")
			oscQueryServer.service.addServiceListener(
				"_oscjson._tcp.local.",
				onServiceAdded = ::serviceAdded,
				onServiceRemoved = ::serviceRemoved,
			)
		} catch (e: IOException) {
			LogManager.warning("[VRCOSCQueryHandler] " + e.message)
		}
	}

	/**
	 * Updates the OSC service's port
	 */
	fun updateOSCQuery(port: UShort) {
		if (oscQueryServer.oscPort != port) {
			thread(start = true) {
				oscQueryServer.updateOscService(port)
			}
		}
	}

	/**
	 * Called when an _oscjson._tcp service is added
	 */
	private fun serviceAdded(info: ServiceInfo) {
		// Check the service name
		if (!info.name.startsWith(serviceStartsWith)) return

		// Prefer IPv4 site-local address from mDNS
		val ip = info.inetAddresses
			.filterIsInstance<Inet4Address>()
			.firstOrNull { it.isSiteLocalAddress }
			?: info.inetAddresses.firstOrNull()
			?: return LogManager.warning("[VRCOSCQueryHandler] No addresses found for service ${info.name}")

		val ipString = ip.hostAddress
		val httpPort = info.port

		LogManager.info("[VRCOSCQueryHandler] Discovered VRChat OSCQuery: ${info.name} at $ipString:$httpPort")

		thread(start = true) {
			try {
				// Fetch HOST_INFO to get the actual OSC port
				val hostInfo = fetchHostInfo(ipString, httpPort)
				val oscPort = hostInfo.oscPort?.toInt()
				if (oscPort == null) {
					LogManager.warning("[VRCOSCQueryHandler] HOST_INFO from ${info.name} did not contain OSC_PORT")
					return@thread
				}

				LogManager.info("[VRCOSCQueryHandler] VRChat ${info.name} OSC port: $oscPort (from HOST_INFO)")

				// Determine which local interface can reach VRChat
				val localAddress = try {
					DatagramSocket().use { sock ->
						sock.connect(ip, httpPort)
						sock.localAddress
					}
				} catch (e: Exception) {
					LogManager.warning("[VRCOSCQueryHandler] Could not determine local interface for $ipString: $e")
					null
				}

				// Publish SlimeVR's mDNS records on the correct interface
				if (localAddress != null && !localAddress.isAnyLocalAddress) {
					oscQueryServer.setPublishAddress(localAddress)
					LogManager.info("[VRCOSCQueryHandler] Publishing mDNS on interface ${localAddress.hostAddress}")
				}

				// Create OSC sender to VRChat using mDNS IP + HOST_INFO port
				vrcOscHandler.addOSCQuerySender(oscPort, ipString)
			} catch (e: Exception) {
				LogManager.warning("[VRCOSCQueryHandler] Failed to connect to VRChat OSCQuery at $ipString:$httpPort: $e")
			}
		}
	}

	/**
	 * Called when an _oscjson._tcp service is removed
	 */
	private fun serviceRemoved(type: String, name: String) {
		if (!name.startsWith(serviceStartsWith)) return
		LogManager.info("[VRCOSCQueryHandler] VRChat OSCQuery service removed: $name")
		vrcOscHandler.closeOscQuerySender(false)
	}

	/**
	 * Closes the OSCQueryServer and the associated OSC sender.
	 */
	fun close() {
		vrcOscHandler.closeOscQuerySender(false)
		thread(start = true) {
			oscQueryServer.close()
		}
	}
}
