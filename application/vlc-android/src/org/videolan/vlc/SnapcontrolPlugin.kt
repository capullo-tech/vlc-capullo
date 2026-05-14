/*****************************************************************************
 * SnapcontrolPlugin.kt
 *
 * Implements the Snapcast stream-plugin JSON-RPC 2.0 protocol on behalf of
 * VLC's PlaybackService. snapserver spawns the libsnapcontrol.so binary as
 * a `controlscript`; that binary is a stdio<->Unix-abstract-socket proxy and
 * connects to the abstract socket bound here. This class accepts connections,
 * sends Plugin.Stream.Ready, answers Plugin.Stream.Player.{Control,
 * SetProperty,GetProperties}, and pushes Plugin.Stream.Player.Properties
 * notifications on discrete player events.
 *
 * The "mute" property is plugin-managed (PlaybackService has no first-class
 * mute API): mute=true caches the last volume and sets volume=0; mute=false
 * restores the cached volume. Volume changes from other UI surfaces while
 * muted will leave the flag stale.
 *****************************************************************************/

package org.videolan.vlc

import android.net.LocalServerSocket
import android.net.LocalSocket
import android.net.Uri
import android.support.v4.media.session.PlaybackStateCompat
import android.util.Base64
import android.util.Log
import androidx.annotation.MainThread
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import org.videolan.libvlc.MediaPlayer
import org.videolan.libvlc.interfaces.IMedia
import java.io.BufferedReader
import java.io.File
import java.io.IOException
import java.io.InputStreamReader

private const val TAG = "SnapcontrolPlugin"
private const val SOCKET_NAME = "snapcontrol"
private const val NOTIFY_READY = """{"jsonrpc":"2.0","method":"Plugin.Stream.Ready"}"""

class SnapcontrolPlugin(
    private val service: PlaybackService,
) : PlaybackService.Callback {

    private val pluginJob = SupervisorJob(service.coroutineContext[Job])
    private val scope = CoroutineScope(service.coroutineContext + pluginJob)

    private var listener: LocalServerSocket? = null

    @Volatile
    private var currentSession: SnapcontrolSession? = null

    fun start() {
        if (listener != null) return
        listener = try {
            LocalServerSocket(SOCKET_NAME)
        } catch (e: IOException) {
            Log.e(TAG, "Failed to bind abstract:$SOCKET_NAME", e)
            return
        }
        Log.d(TAG, "listening on abstract:$SOCKET_NAME")
        service.addCallback(this)
        scope.launch { acceptLoop() }
    }

    fun stop() {
        service.removeCallback(this)
        val srv = listener
        listener = null
        try { srv?.close() } catch (_: IOException) {}
        currentSession?.close()
        currentSession = null
        pluginJob.cancel()
    }

    private suspend fun acceptLoop() {
        val srv = listener ?: return
        while (scope.isActive) {
            val sock = try {
                srv.accept()
            } catch (e: IOException) {
                Log.d(TAG, "accept loop ending: ${e.message}")
                return
            }
            Log.d(TAG, "session accepted")
            val session = SnapcontrolSession(sock, service, scope)
            currentSession = session
            try {
                session.run()
            } finally {
                currentSession = null
                Log.d(TAG, "session ended")
            }
        }
    }

    override fun update() {
        currentSession?.notifyPropertiesFromMain()
    }

    override fun onMediaEvent(event: IMedia.Event) {}

    override fun onMediaPlayerEvent(event: MediaPlayer.Event) {
        val session = currentSession ?: return
        when (event.type) {
            MediaPlayer.Event.Playing,
            MediaPlayer.Event.Paused,
            MediaPlayer.Event.EndReached,
            MediaPlayer.Event.MediaChanged,
            MediaPlayer.Event.LengthChanged,
            MediaPlayer.Event.PausableChanged,
            MediaPlayer.Event.SeekableChanged -> session.notifyPropertiesFromMain()
            MediaPlayer.Event.EncounteredError -> session.sendLogFromMain("error", "VLC playback error")
        }
    }
}

private class SnapcontrolSession(
    private val socket: LocalSocket,
    private val service: PlaybackService,
    parentScope: CoroutineScope,
) {
    private val outbox = Channel<String>(Channel.UNLIMITED)
    private val sessionJob = SupervisorJob(parentScope.coroutineContext[Job])
    private val scope = CoroutineScope(parentScope.coroutineContext + sessionJob)

    private var muted: Boolean = false
    private var lastVolumeBeforeMute: Int? = null

    // Artwork cache: VLC's mw.artworkMrl points at a local file:// path that snap
    // clients on other hosts can't fetch. For local files we read+base64 the bytes
    // and emit them as `artData`; snapserver decodes/caches them and republishes
    // a working `artUrl` to clients. http(s) MRLs are emitted as `artUrl` directly.
    @Volatile private var cachedArtMrl: String? = null
    @Volatile private var cachedArtUrl: String? = null
    @Volatile private var cachedArtData: JSONObject? = null

    suspend fun run() {
        outbox.trySend(NOTIFY_READY)
        val writerJob = scope.launch(Dispatchers.IO) { writerLoop() }
        try {
            withContext(Dispatchers.Main) { refreshArtworkCache() }
            withContext(Dispatchers.IO) { readerLoop() }
        } finally {
            outbox.close()
            writerJob.cancel()
            try { socket.close() } catch (_: IOException) {}
            sessionJob.cancel()
        }
    }

    fun close() {
        try { socket.close() } catch (_: IOException) {}
        outbox.close()
        sessionJob.cancel()
    }

    @MainThread
    fun notifyPropertiesFromMain() {
        try {
            refreshArtworkCache()
            val notif = JSONObject()
                .put("jsonrpc", "2.0")
                .put("method", "Plugin.Stream.Player.Properties")
                .put("params", buildProperties())
            outbox.trySend(notif.toString())
        } catch (t: Throwable) {
            Log.w(TAG, "buildProperties failed", t)
        }
    }

    @MainThread
    private fun refreshArtworkCache() {
        val mrl = service.currentMediaWrapper?.artworkMrl
        if (mrl == cachedArtMrl) return
        cachedArtMrl = mrl
        cachedArtUrl = null
        cachedArtData = null
        if (mrl.isNullOrEmpty()) return
        val decoded = Uri.decode(mrl)
        if (decoded.startsWith("http://") || decoded.startsWith("https://")) {
            cachedArtUrl = decoded
            return
        }
        val path = decoded.removePrefix("file://")
        scope.launch(Dispatchers.IO) {
            try {
                val file = File(path)
                if (!file.exists() || file.length() == 0L) return@launch
                val bytes = file.readBytes()
                val ext = file.extension.lowercase().ifEmpty { "jpg" }
                val payload = JSONObject()
                    .put("data", Base64.encodeToString(bytes, Base64.NO_WRAP))
                    .put("extension", ext)
                // Only commit if we're still on the same track (no MediaChanged raced past us)
                if (cachedArtMrl == mrl) {
                    cachedArtData = payload
                    withContext(Dispatchers.Main) { notifyPropertiesFromMain() }
                }
            } catch (t: Throwable) {
                Log.w(TAG, "art read failed for $path: ${t.message}")
            }
        }
    }

    @MainThread
    fun sendLogFromMain(severity: String, message: String) {
        val notif = JSONObject()
            .put("jsonrpc", "2.0")
            .put("method", "Plugin.Stream.Log")
            .put("params", JSONObject().put("severity", severity).put("message", message))
        outbox.trySend(notif.toString())
    }

    private suspend fun readerLoop() {
        val reader = BufferedReader(InputStreamReader(socket.inputStream, Charsets.UTF_8))
        while (scope.isActive) {
            val line = try {
                reader.readLine() ?: return
            } catch (e: IOException) {
                Log.d(TAG, "reader end: ${e.message}")
                return
            }
            if (line.isBlank()) continue
            handleLine(line)
        }
    }

    private suspend fun writerLoop() {
        val out = socket.outputStream
        try {
            for (line in outbox) {
                Log.v(TAG, "tx: $line")
                out.write(line.toByteArray(Charsets.UTF_8))
                out.write('\n'.code)
                out.flush()
            }
        } catch (e: IOException) {
            Log.d(TAG, "writer end: ${e.message}")
        }
    }

    private suspend fun handleLine(line: String) {
        Log.v(TAG, "rx: $line")
        val req = try {
            JSONObject(line)
        } catch (e: JSONException) {
            Log.w(TAG, "JSON parse error: ${e.message}")
            return
        }
        val id: Any? = if (req.has("id") && !req.isNull("id")) req.get("id") else null
        val method = req.optString("method", "")
        if (method.isEmpty()) {
            Log.w(TAG, "missing method: $line")
            return
        }
        try {
            val result: Any = when (method) {
                "Plugin.Stream.Player.Control" -> {
                    handleControl(req.optJSONObject("params") ?: JSONObject())
                    "ok"
                }
                "Plugin.Stream.Player.SetProperty" -> {
                    handleSetProperty(req.optJSONObject("params") ?: JSONObject())
                    "ok"
                }
                "Plugin.Stream.Player.GetProperties" -> {
                    withContext(Dispatchers.Main) { buildProperties() }
                }
                else -> {
                    if (id != null) sendError(id, -32601, "Method not found: $method")
                    return
                }
            }
            if (id != null) sendResult(id, result)
        } catch (e: JSONException) {
            if (id != null) sendError(id, -32602, "Invalid params: ${e.message}")
        } catch (e: Throwable) {
            Log.e(TAG, "dispatch error", e)
            if (id != null) sendError(id, -32603, e.message ?: e.javaClass.simpleName)
        }
    }

    private suspend fun handleControl(params: JSONObject) {
        val command = params.getString("command")
        val args = params.optJSONObject("params") ?: JSONObject()
        withContext(Dispatchers.Main) {
            when (command) {
                "play" -> service.play()
                "pause" -> service.pause()
                "playPause" -> if (service.isPlaying) service.pause() else service.play()
                // snapcast "stop" means stop playback, not tear down the plugin —
                // PlaybackService.stop() would also call stopSnapserver(), which
                // would cancel this very coroutine. Pause is the safe equivalent.
                "stop" -> service.pause()
                "next" -> service.next()
                "previous" -> service.previous(false)
                "seek" -> {
                    val offsetMs = (args.getDouble("offset") * 1000.0).toLong()
                    service.seek(service.getTime() + offsetMs)
                }
                "setPosition" -> {
                    val positionMs = (args.getDouble("position") * 1000.0).toLong()
                    service.seek(positionMs)
                }
                else -> throw IllegalArgumentException("Unknown control command: $command")
            }
        }
    }

    private suspend fun handleSetProperty(params: JSONObject) {
        withContext(Dispatchers.Main) {
            val keys = params.keys()
            while (keys.hasNext()) {
                val key = keys.next()
                when (key) {
                    "loopStatus" -> service.repeatType = when (params.getString("loopStatus")) {
                        "none" -> PlaybackStateCompat.REPEAT_MODE_NONE
                        "track" -> PlaybackStateCompat.REPEAT_MODE_ONE
                        "playlist" -> PlaybackStateCompat.REPEAT_MODE_ALL
                        else -> throw IllegalArgumentException("Unknown loopStatus")
                    }
                    "shuffle" -> {
                        val want = params.getBoolean("shuffle")
                        if (want != service.isShuffling) service.shuffle()
                    }
                    "volume" -> {
                        val v = params.getInt("volume").coerceIn(0, 100)
                        service.setVolume(v)
                        muted = false
                        lastVolumeBeforeMute = null
                    }
                    "mute" -> {
                        val want = params.getBoolean("mute")
                        if (want && !muted) {
                            lastVolumeBeforeMute = service.volume
                            service.setVolume(0)
                            muted = true
                        } else if (!want && muted) {
                            service.setVolume(lastVolumeBeforeMute ?: 100)
                            muted = false
                            lastVolumeBeforeMute = null
                        }
                    }
                    "rate" -> service.setRate(params.getDouble("rate").toFloat(), false)
                    else -> Log.w(TAG, "Unsupported property: $key")
                }
            }
        }
    }

    @MainThread
    private fun buildProperties(): JSONObject {
        val playbackStatus = when {
            service.isPlaying -> "playing"
            service.isPaused -> "paused"
            else -> "stopped"
        }
        val loopStatus = when (service.repeatType) {
            PlaybackStateCompat.REPEAT_MODE_ONE -> "track"
            PlaybackStateCompat.REPEAT_MODE_ALL,
            PlaybackStateCompat.REPEAT_MODE_GROUP -> "playlist"
            else -> "none"
        }
        val obj = JSONObject()
            .put("playbackStatus", playbackStatus)
            .put("loopStatus", loopStatus)
            .put("shuffle", service.isShuffling)
            .put("volume", service.volume.coerceIn(0, 100))
            .put("mute", muted)
            .put("rate", service.rate.toDouble())
            .put("position", service.getTime() / 1000.0)
            .put("canPlay", service.hasMedia())
            .put("canPause", service.isPausable)
            .put("canSeek", service.isSeekable)
            .put("canGoNext", service.hasNext())
            .put("canGoPrevious", service.hasPrevious())
            .put("canControl", true)
        service.currentMediaWrapper?.let { mw ->
            val meta = JSONObject()
            mw.title?.takeIf { it.isNotEmpty() }?.let { meta.put("title", it) }
            mw.artistName?.takeIf { it.isNotEmpty() }?.let { meta.put("artist", JSONArray().put(it)) }
            mw.albumName?.takeIf { it.isNotEmpty() }?.let { meta.put("album", it) }
            cachedArtUrl?.let { meta.put("artUrl", it) }
            cachedArtData?.let { meta.put("artData", it) }
            mw.length.takeIf { it > 0L }?.let { meta.put("duration", it / 1000.0) }
            mw.id.takeIf { it != 0L }?.let { meta.put("trackId", it.toString()) }
            obj.put("metadata", meta)
        }
        return obj
    }

    private fun sendResult(id: Any, result: Any) {
        val resp = JSONObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", result)
        outbox.trySend(resp.toString())
    }

    private fun sendError(id: Any, code: Int, message: String) {
        val resp = JSONObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("error", JSONObject().put("code", code).put("message", message))
        outbox.trySend(resp.toString())
    }
}
