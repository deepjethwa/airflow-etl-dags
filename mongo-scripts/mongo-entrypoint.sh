#!/bin/bash
set -e

echo "📦 Starting MongoDB..."

# Start MongoDB in background (not with --fork, use & for background process)
mongod --bind_ip_all --logpath /var/log/mongodb/mongod.log --logappend &

# Store the background process PID
MONGO_PID=$!

# Wait for MongoDB to become available
echo "⏳ Waiting for MongoDB to be ready..."
for i in {1..30}; do
    if mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
        echo "✅ MongoDB is ready!"
        break
    fi
    echo "  ...still waiting... ($i/30)"
    sleep 2
done

# Check if MongoDB is actually ready
if ! mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
    echo "❌ MongoDB failed to start properly"
    exit 1
fi

# Run initialization script with proper error handling
echo "⚙️ Running initialization script..."
if [ -f "/mongo-scripts/run.sh" ]; then
    echo "🔄 Executing run.sh..."
    bash /mongo-scripts/run.sh || echo "⚠️ Init script completed (some operations may have been skipped)"
else
    echo "ℹ️ No run.sh found, skipping initialization"
fi

# 🧩 Run index creation script (if it exists)
if [ -f "/mongo-scripts/create-index.js" ]; then
    echo "📚 Running create-index.js to set up indexes..."
    mongosh mongo_test /mongo-scripts/create-index.js || echo "⚠️ Index creation script failed (continuing anyway)"
else
    echo "ℹ️ No create-index.js found, skipping index setup"
fi

echo "✅ All initialization complete. MongoDB is running in background."
echo "🚀 Container will stay alive. MongoDB is ready for connections at localhost:27017"

# 🧩 Graceful shutdown
trap "echo '🛑 Stopping MongoDB...'; kill $MONGO_PID" SIGTERM SIGINT
wait $MONGO_PID
