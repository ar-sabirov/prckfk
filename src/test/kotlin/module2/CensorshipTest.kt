package module2

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Locale

class CensorshipTest {
    @Test
    fun `rule and message normalization use ROOT even with a Turkish default locale`() {
        val previousLocale = Locale.getDefault()
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"))
            assertEquals("i", normalizeStopWord(" I "))
            assertEquals("привет", normalizeStopWord("\tПРИВЕТ\n"))
            assertEquals("*** Ibis!", censor("I Ibis!", setOf("i")))
        } finally {
            Locale.setDefault(previousLocale)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["", "  ", "bad word", "bad!", "bad-word", ".*"])
    fun `stop word commands reject empty words phrases and punctuation`(input: String) {
        assertThrows(IllegalArgumentException::class.java) { normalizeStopWord(input) }
    }
}
