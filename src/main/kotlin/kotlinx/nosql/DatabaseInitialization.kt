package kotlinx.nosql

abstract class DatabaseInitialization<S : Session>() {
}

class Create<S: Session>(val onCreate: S.() -> Unit = { }) : DatabaseInitialization<S>()

class CreateDrop<S: Session>(val onCreate: S.() -> Unit = { },
             onDrop: S.() -> Unit = { }) : DatabaseInitialization<S>()

class Update<S: Session>() : DatabaseInitialization<S>()

class Validate<S: Session>() : DatabaseInitialization<S>()