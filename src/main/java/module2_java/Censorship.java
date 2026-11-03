package module2_java;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public final class Censorship {
    // Use the same word definition for validation and tokenization.
    // Whole-token matching preserves separators and never masks substrings.
    private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{M}\\p{N}_]+");

    private Censorship() {}

    public static String normalizeStopWord(String word) {
        // Match Kotlin's trim(), including non-breaking Unicode spaces.
        int start = 0;
        int end = word.length();
        while (start < end && isWhitespace(word.charAt(start))) {
            start++;
        }
        while (end > start && isWhitespace(word.charAt(end - 1))) {
            end--;
        }
        String normalized = word.substring(start, end).toLowerCase(Locale.ROOT);
        if (!WORD.matcher(normalized).matches()) {
            throw new IllegalArgumentException(
                    "Stop word must be one non-empty word containing only letters, combining marks, numbers, or underscores");
        }
        return normalized;
    }

    public static String censor(String text, Set<String> stopWords) {
        return WORD.matcher(text).replaceAll(match ->
                stopWords.contains(match.group().toLowerCase(Locale.ROOT)) ? "***" : match.group());
    }

    private static boolean isWhitespace(char character) {
        return Character.isWhitespace(character) || Character.isSpaceChar(character);
    }
}
