#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

echo "Initializing RabbitMQ default users..."

# Read login and password from secrets
RABBITMQ_DEFAULT_USER=$(cat /run/secrets/rabbitmq_user)
RABBITMQ_DEFAULT_PASS=$(cat /run/secrets/rabbitmq_pass)

# Connecting to main RabbitMQ node
RABBITMQ_NODE="rabbit@rabbitmq"  # Basic name of the main node
                                 # Should be resolved by container's orchestrator

# Waiting for main node to receive commands
echo "Waiting for RabbitMQ to be ready..."
until rabbitmqctl -n $RABBITMQ_NODE ping; do
  sleep 5
done

# Sleeping for some additional time to ensure the node can receive commands
sleep 10

echo "Creating default user..."
# Create user in the main node
rabbitmqctl -n $RABBITMQ_NODE add_user "$RABBITMQ_DEFAULT_USER" "$RABBITMQ_DEFAULT_PASS"
# Assign administrator role to the user
rabbitmqctl -n $RABBITMQ_NODE set_user_tags "$RABBITMQ_DEFAULT_USER" administrator
# Grant user all permissions in the virtual host "/"
rabbitmqctl -n $RABBITMQ_NODE set_permissions -p / "$RABBITMQ_DEFAULT_USER" ".*" ".*" ".*"

echo "Default user successfully configured!"