package module2

import java.util.Locale

// Use the same word definition for rule validation and message tokenization.
// Matching complete tokens preserves separators and avoids masking substrings.
private val WORD = Regex("""[\p{L}\p{M}\p{N}_]+""")

fun normalizeStopWord(word: String): String {
    val normalized = word.trim().lowercase(Locale.ROOT)
    require(WORD.matches(normalized)) {
        "Stop word must be one non-empty word containing only letters, combining marks, numbers, or underscores"
    }
    return normalized
}

fun censor(text: String, stopWords: Set<String>): String = WORD.replace(text) { match ->
    if (match.value.lowercase(Locale.ROOT) in stopWords) "***" else match.value
}
