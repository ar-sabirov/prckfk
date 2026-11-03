package module2

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration

const val DEFAULT_SENTENCE_SOURCE_URL =
    "https://gist.githubusercontent.com/romaklimenko/c95f3a864828f7f034b7a33d1676e420/raw/warandpeace.txt"

fun loadSentences(sourceUrl: String): List<String> {
    val request = HttpRequest.newBuilder(URI.create(sourceUrl))
        .timeout(Duration.ofSeconds(30))
        .header("Accept", "text/plain")
        .GET()
        .build()
    val text = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build().use { client ->
            val response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
            check(response.statusCode() == 200) {
                "Sentence source returned HTTP ${response.statusCode()}: $sourceUrl"
            }
            response.body()
        }
    return splitSentences(text).also {
        check(it.isNotEmpty()) { "No sentences found in $sourceUrl; supply a UTF-8 plain-text source" }
    }
}

fun splitSentences(text: String): List<String> = text.split('.')
    .map { it.trim() }
    .filter { it.isNotEmpty() }
