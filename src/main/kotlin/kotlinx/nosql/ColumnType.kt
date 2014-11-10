package kotlinx.nosql

enum class ColumnType(val primitive: Boolean = false,
                      val iterable: Boolean = false,
                      val list: Boolean = false,
                      val set: Boolean = false,
                      val custom: Boolean = false,
                      val id: Boolean = false) {
    INTEGER : ColumnType(primitive = true)
    PRIMARY_ID : ColumnType(primitive = true, id = true)
    FOREIGN_ID : ColumnType(primitive = true, id = true)
    STRING : ColumnType(primitive = true)
    BOOLEAN : ColumnType(primitive = true)
    DATE : ColumnType(primitive = true)
    TIME : ColumnType(primitive = true)
    DATE_TIME : ColumnType(primitive = true)
    DOUBLE : ColumnType(primitive = true)
    FLOAT : ColumnType(primitive = true)
    LONG : ColumnType(primitive = true)
    SHORT : ColumnType(primitive = true)
    BYTE : ColumnType(primitive = true)
    INTEGER_SET : ColumnType(iterable = true, set = true)
    ID_SET : ColumnType(iterable = true, set = true, id = true)
    ID_LIST : ColumnType(iterable = true, list = true, id = true)
    STRING_SET : ColumnType(iterable = true, set = true)
    INTEGER_LIST : ColumnType(iterable = true, list = true)
    STRING_LIST : ColumnType(iterable = true, list = true)
    CUSTOM_CLASS : ColumnType(custom = true)
    CUSTOM_CLASS_LIST : ColumnType(iterable = true, custom = true, list = true)
    CUSTOM_CLASS_SET : ColumnType(iterable = true, custom = true, set = true)
}
