package kotlinx.nosql

abstract class DatabaseInitialization<S : Session>() {
}

class Create<S: Session>(val create: S.() -> Unit = { }) : DatabaseInitialization<S>()

class CreateDrop<S: Session>(val create: S.() -> Unit = { },
             drop: S.() -> Unit = { }) : DatabaseInitialization<S>()

class Update<S: Session>(val create: S.() -> Unit = { }) : DatabaseInitialization<S>()

class Validate<S: Session>() : DatabaseInitialization<S>()