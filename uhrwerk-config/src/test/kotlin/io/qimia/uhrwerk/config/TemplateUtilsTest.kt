package io.qimia.uhrwerk.config

import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.common.utils.TemplateUtils.templateArgs
import org.junit.jupiter.api.Test

class TemplateUtilsTest {
    @Test
    fun argsTest() {
        val template =
            "SELECT *, \$source_variable2\$ as fix_val_col FROM table WHERE column = \$source_variable1\$ " +
                    "AND column2 = 'something-with-dollar' " +
                    "AND column3 = '\$source_variable2\$'"
        val args = templateArgs(template)
        assertThat(args).isNotEmpty()
        assertThat(args).containsExactly("source_variable1", "source_variable2")
    }
}