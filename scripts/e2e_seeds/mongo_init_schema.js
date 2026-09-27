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

if (db.getCollectionNames().indexOf("user_events") === -1) {
    // Collection 3: User Events
    db.createCollection("user_events");

    db.user_events.insertMany([
        { id: "evt_1", user_id: 101, event_type: "page_view", timestamp: new Date() },
        { id: "evt_2", user_id: 102, event_type: "add_to_cart", timestamp: new Date() },
        { id: "evt_3", user_id: 103, event_type: "checkout", timestamp: new Date() }
    ]);
}

if (db.getCollectionNames().indexOf("clickstream") === -1) {
    // Collection 4: Clickstream
    db.createCollection("clickstream");

    db.clickstream.insertMany([
        { id: "clk_1", session_id: "s_1001", url: "/home", duration_sec: 45, timestamp: new Date() },
        { id: "clk_2", session_id: "s_1002", url: "/products", duration_sec: 120, timestamp: new Date() }
    ]);
}
