# Kafka topics used across services

ORDERS_CREATED = "orders.created" # Topic for new orders created by the Order Service
INVENTORY_RESERVED = "inventory.reserved" # Topic for inventory reserved by the Inventory Service in response to new orders
INVENTORY_REJECTED = "inventory.rejected" # Topic for inventory reservation failures (e.g., out of stock) sent by the Inventory Service
ORDERS_CONFIRMED = "orders.confirmed" # Topic for orders that have been confirmed (inventory reserved successfully) sent by the Order Service
ORDERS_REJECTED = "orders.rejected" # Topic for orders that have been rejected (e.g., due to inventory reservation failure) sent by the Order Service


DLQ_SUFFIX = ".dlq"

OUTBOX_PREFIX = "outbox:"
OUTBOX_PREFIX_CONSTANT = "outbox."  # Prefix for outbox topics used in the Outbox pattern
