package io.qimia.uhrwerk.config.representation


data class FunctionCall(
    var name: String? = null,
    var args: Array<FunctionArgument>? = null,
    var inputs: Array<InputView>? = null,
    var output: String? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionCall) return false

        if (name != other.name) return false
        if (args != null) {
            if (other.args == null) return false
            if (!args.contentEquals(other.args)) return false
        } else if (other.args != null) return false
        if (inputs != other.inputs) return false
        return output == other.output
    }

    override fun hashCode(): Int {
        var result = name?.hashCode() ?: 0
        result = 31 * result + (args?.contentHashCode() ?: 0)
        result = 31 * result + (inputs?.hashCode() ?: 0)
        result = 31 * result + (output?.hashCode() ?: 0)
        return result
    }

}
