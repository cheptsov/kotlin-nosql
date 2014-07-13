package kotlinx.nosql

enum class ColumnType(val primitive: Boolean = false,
                      val iterable: Boolean = false,
                      val list: Boolean = false,
                      val set: Boolean = false,
                      val custom: Boolean = false,
                      val id: Boolean = false) {
    public INTEGER : ColumnType(primitive = true)
    public PRIMARY_ID : ColumnType(primitive = true, id = true)
    public FOREIGN_ID : ColumnType(primitive = true, id = true)
    public ID : ColumnType(primitive = true)
    public STRING : ColumnType(primitive = true)
    public BOOLEAN : ColumnType(primitive = true)
    public DATE : ColumnType(primitive = true)
    public TIME : ColumnType(primitive = true)
    public DATE_TIME : ColumnType(primitive = true)
    public DOUBLE : ColumnType(primitive = true)
    public FLOAT : ColumnType(primitive = true)
    public LONG : ColumnType(primitive = true)
    public SHORT : ColumnType(primitive = true)
    public BYTE : ColumnType(primitive = true)
    public INTEGER_SET : ColumnType(iterable = true, set = true)
    public ID_SET : ColumnType(iterable = true, set = true, id = true)
    public ID_LIST : ColumnType(iterable = true, list = true, id = true)
    public STRING_SET : ColumnType(iterable = true, set = true)
    public INTEGER_LIST : ColumnType(iterable = true, list = true)
    public STRING_LIST : ColumnType(iterable = true, list = true)
    public CUSTOM_CLASS : ColumnType(custom = true)
    public CUSTOM_CLASS_LIST : ColumnType(iterable = true, custom = true, list = true)
    public CUSTOM_CLASS_SET : ColumnType(iterable = true, custom = true, set = true)
}