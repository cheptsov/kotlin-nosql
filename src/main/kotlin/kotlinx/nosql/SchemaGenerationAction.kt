package kotlinx.nosql

abstract class SchemaGenerationAction<S : Session>() {
}

class Create<S: Session>(val onCreate: S.() -> Unit = { }) : SchemaGenerationAction<S>()

class CreateDrop<S: Session>(val onCreate: S.() -> Unit = { },
             onDrop: S.() -> Unit = { }) : SchemaGenerationAction<S>()

class Update<S: Session>() : SchemaGenerationAction<S>()

class Validate<S: Session>() : SchemaGenerationAction<S>()
