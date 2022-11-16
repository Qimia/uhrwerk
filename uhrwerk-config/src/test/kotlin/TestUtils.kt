object TestUtils {
    fun fileToString(fileName: String) =
        this::class.java.getResourceAsStream(fileName)
            ?.bufferedReader()?.readText()

    fun filePath(fileName: String) =
        this::class.java.getResource(fileName)?.path
}