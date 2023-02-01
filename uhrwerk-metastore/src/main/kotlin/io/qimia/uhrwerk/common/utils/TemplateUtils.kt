package io.qimia.uhrwerk.common.utils

import org.stringtemplate.v4.ST
import org.stringtemplate.v4.compiler.STParser
import java.util.*

object TemplateUtils {
    @JvmStatic
    fun templateArgs(template: String): SortedSet<String> {
        val args = sortedSetOf<String>()
        val st = ST(template, '$', '$')
        val tSize = st.impl.tokens.size()
        for (i in 0 until tSize) {
            val token = st.impl.tokens[i]
            if (token.type == STParser.ID) {
                args.add(token.text)
            }
        }
        return args
    }

    @JvmStatic
    fun renderTemplate(template: String, args: Map<String, String>): String? {
        val st = ST(template, '$', '$')
        for ((key, value) in args) {
            st.add(key, value)
        }
        return st.render()
    }
}