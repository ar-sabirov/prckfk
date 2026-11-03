#!/bin/sh
set -eu

# Do not reset membership or rules when Compose starts an existing demo again.
if [ -f /var/lib/chat-demo/initialized ]; then
    echo "Demo data already initialized"
    exit 0
fi

chat_command() {
    java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp /app/app.jar module2_java.ChatCommands "$@"
}

chat_command add Alice Bob Степан
chat_command add-stop-word bad
chat_command block Alice Bob

touch /var/lib/chat-demo/initialized
echo "Demo data initialized"
