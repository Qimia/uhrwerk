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
    fun renderTemplate(template: String, args: Map<String, Object>): String? {
        val st = ST(template, '$', '$')
        if (!args.isNullOrEmpty())
            for ((key, value) in args) {
                if (st.attributes.isNullOrEmpty() || !st.attributes.containsKey(key))
                    st.add(key, value)
            }
        return st.render()
    }
}