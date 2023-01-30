package io.qimia.uhrwerk.config.utils

import org.stringtemplate.v4.ST

object TemplateUtils {
    fun templateArgs(template: String): Array<String> {
        val args = mutableListOf<String>()
        val st = ST(template, '$', '$')
        return args.toTypedArray()
    }
}