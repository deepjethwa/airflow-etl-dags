#!/bin/bash
echo "🔄 Checking and creating MongoDB collections if they don't exist..."

mongosh mongo_test --eval "
try {
    if (!db.getCollectionNames().includes('students')) {
        db.createCollection('students');
        print('✅ Created students collection');
    } else {
        print('ℹ️ Collection students already exists — skipping');
    }

    if (!db.getCollectionNames().includes('students_1')) {
        db.createCollection('students_1');
        print('✅ Created students_1 collection');
    } else {
        print('ℹ️ Collection students_1 already exists — skipping');
    }

    print('✅ All collections checked/created successfully');
} catch (error) {
    print('⚠️ Error during collection setup: ' + error);
}
"

echo "✅ MongoDB collection check complete."
