db = db.getSiblingDB('test_db');

if (db.getCollectionNames().indexOf("users_strict") === -1) {
    // Collection 1: Strict JSON Schema
    db.createCollection("users_strict", {
        validator: {
            $jsonSchema: {
                bsonType: "object",
                required: ["name", "email", "age"],
                properties: {
                    name: { bsonType: "string", description: "must be a string and is required" },
                    email: { bsonType: "string", description: "must be a string and is required" },
                    age: { bsonType: "int", minimum: 0, description: "must be an integer and is required" }
                }
            }
        }
    });

    db.users_strict.insertMany([
        { name: "Alice", email: "alice@test.com", age: NumberInt(30) },
        { name: "Bob", email: "bob@test.com", age: NumberInt(25) }
    ]);
}

if (db.getCollectionNames().indexOf("logs_loose") === -1) {
    // Collection 2: Loose / Schemaless
    db.createCollection("logs_loose");

    db.logs_loose.insertMany([
        { level: "INFO", message: "Server started", timestamp: new Date() },
        { level: "ERROR", message: "Connection lost", code: 500 },
        { user_id: 123, action: "login", success: true }
    ]);
}
