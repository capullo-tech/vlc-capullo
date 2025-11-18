package org.videolan.vlc

import android.content.Context
import android.os.Build
import android.system.Os.mkfifo
import android.system.OsConstants.S_IRUSR
import android.system.OsConstants.S_IWUSR
import android.util.Log
import androidx.annotation.RequiresApi
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader

class SnapserverProcess(private val context: Context) {

    private val nativeLibDir = getNativeLibDirPath()
    private val cacheDir = getCacheDir()
    private val confFile = getSnapserverConfPath()
    @RequiresApi(Build.VERSION_CODES.LOLLIPOP)
    private val pipeFilepath = getPipeFilepath() ?: throw IllegalStateException("Failed to create pipe")

    companion object {
        private const val STREAM_NAME: String = "name=VLCAndroid"
        private const val PIPE_MODE: String = "mode=read"
        private const val DRYOUT_MS: String = "dryout_ms=2000"
        private const val SAMPLE_FORMAT: String = "sampleformat=44100:16:2"
        private const val PIPE_NAME = "filifo"

        private val pipeArgs = listOf(
            STREAM_NAME,
            PIPE_MODE,
            DRYOUT_MS,
            SAMPLE_FORMAT,
        ).joinToString("&")

        private val TAG = SnapserverProcess::class.java.simpleName
    }

    @RequiresApi(Build.VERSION_CODES.LOLLIPOP)
    private fun getPipeFilepath(): String? {
        val pipeFile = File(getCacheDir(), PIPE_NAME)
        return pipeFile.absolutePath
    }

    private fun getNativeLibDirPath(): String = context.applicationInfo.nativeLibraryDir

    private fun getCacheDir(): File = context.cacheDir

    private fun getFilesDir(): File = context.filesDir

    // Add empty/dummy snapserver conf file, all settings are specified as cli args on the
    // SnapserverProcess
    private fun getSnapserverConfPath(): String {
        val confFile = File(getCacheDir(), "snapserver.conf")

        if (!confFile.exists()) {
            try {
                confFile.createNewFile()
                Log.d(TAG, "Created snapserver.conf: ${confFile.absolutePath}")
            } catch (e: Exception) {
                Log.e(TAG, "Error creating snapserver.conf: ${e.message}")
            }
        }

        return confFile.absolutePath
    }

    @RequiresApi(Build.VERSION_CODES.LOLLIPOP)
    suspend fun start() = coroutineScope {
        val pb = ProcessBuilder()
            .command(
                "$nativeLibDir/libsnapserver.so",
                "--config",
                confFile,
                "--server.datadir=$cacheDir",
                "--stream.source",
                "pipe://$pipeFilepath?$pipeArgs",
            )
            .redirectErrorStream(true)

        val process = pb.start()
        try {
            val bufferedReader = BufferedReader(
                InputStreamReader(process.inputStream),
            )
            var line: String?
            while (bufferedReader.readLine().also { line = it } != null) {
                ensureActive()
                Log.d(TAG, "Snapserver: $line")
            }
        } catch (_: CancellationException) {
            Log.d(TAG, "Snapserver process cancelled")
            process.destroy()
            process.waitFor()
            Log.d(TAG, "Snapserver process destroyed")
        } catch (e: Exception) {
            Log.e(TAG, "Error starting snapcast process", e)
        }
    }
}