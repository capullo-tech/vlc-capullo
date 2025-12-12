package org.videolan.vlc

import android.content.Context
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.os.Build
import android.provider.Settings
import android.util.Log

class SnapserverNsdManager (
    private val applicationContext: Context,
) {

    private val registeredListeners = mutableListOf<NsdManager.RegistrationListener>()

    private val nsdManager = applicationContext.getSystemService(Context.NSD_SERVICE) as NsdManager

    fun getDeviceName(): String = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N_MR1) {
        val deviceName = Settings.Global.getString(
            applicationContext.contentResolver,
            Settings.Global.DEVICE_NAME,
        )
        if (deviceName == Build.MODEL) Build.MODEL else "$deviceName (${Build.MODEL})"
    } else {
        Build.MODEL
    }

    fun start() {
        val controlServiceInfo = NsdServiceInfo().apply {
            serviceName = "$SERVICE_NAME_PREFIX${getDeviceName()} (VLCapullo)"
            serviceType = SERVICE_TYPE
            port = SERVICE_PORT
        }

        val streamServiceInfo = NsdServiceInfo().apply {
            serviceName = "$SERVICE_NAME_PREFIX${getDeviceName()} (VLCapullo)"
            serviceType = STREAM_SERVICE_TYPE
            port = STREAM_SERVICE_PORT
        }

        registerNsdService(controlServiceInfo)
        registerNsdService(streamServiceInfo)
    }

    fun stop() {
        registeredListeners.forEach { listener ->
            try {
                nsdManager.unregisterService(listener)
            } catch (e: Exception) {
                Log.e(TAG, "Error unregistering service", e)
            }
        }
        registeredListeners.clear()
    }

    private fun registerNsdService(serviceInfo: NsdServiceInfo) {
        val registrationListener = object : NsdManager.RegistrationListener {
            override fun onServiceRegistered(nsdServiceInfo: NsdServiceInfo) {
                Log.d(TAG, "Service registered: ${nsdServiceInfo.serviceType}")
            }

            override fun onRegistrationFailed(serviceInfo: NsdServiceInfo, errorCode: Int) {
                Log.e(TAG, "Registration failed for ${serviceInfo.serviceType}: $errorCode")
            }

            override fun onServiceUnregistered(arg0: NsdServiceInfo) {
                Log.d(TAG, "Service unregistered: ${arg0.serviceType}")
            }

            override fun onUnregistrationFailed(serviceInfo: NsdServiceInfo, errorCode: Int) {
                Log.e(TAG, "Unregistration failed for ${serviceInfo.serviceType}: $errorCode")
            }
        }

        registeredListeners.add(registrationListener)

        nsdManager.registerService(
            serviceInfo,
            NsdManager.PROTOCOL_DNS_SD,
            registrationListener,
        )
    }

    companion object {
        private val TAG = SnapserverNsdManager::class.java.simpleName
        const val SERVICE_NAME_PREFIX = "Snapcast - "
        const val SERVICE_TYPE = "_snapcast._tcp"
        const val SERVICE_PORT = 1704
        const val STREAM_SERVICE_TYPE = "_snapcast-stream._tcp"
        const val STREAM_SERVICE_PORT = 1705
    }
}
