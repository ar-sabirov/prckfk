package module2_java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Locale;
import java.util.Set;

import static module2_java.Censorship.censor;
import static module2_java.Censorship.normalizeStopWord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CensorshipTest {
    @Test
    void normalizationUsesRootEvenWithTurkishDefaultLocale() {
        Locale previousLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            assertEquals("i", normalizeStopWord(" I "));
            assertEquals("привет", normalizeStopWord("\tПРИВЕТ\n"));
            assertEquals("*** Ibis!", censor("I Ibis!", Set.of("i")));
        } finally {
            Locale.setDefault(previousLocale);
        }
    }

    @Test
    void normalizationPreservesKotlinUnicodeWhitespaceSemantics() {
        assertEquals("bad", normalizeStopWord("\u00a0\u2007BAD\u202f"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "  ", "bad word", "bad!", "bad-word", ".*"})
    void stopWordsRejectEmptyWordsPhrasesAndPunctuation(String input) {
        assertThrows(IllegalArgumentException.class, () -> normalizeStopWord(input));
    }
}
