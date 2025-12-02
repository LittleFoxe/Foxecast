"""
Custom RabbitMQ Hook for Apache Airflow
(could have used another plugin, but it could be more
useful to set connection through URL instead of separate parameters)
"""
from typing import Any, Optional, Dict
import json
import pika

from airflow.hooks.base import BaseHook


class RabbitMQHook(BaseHook):
    """
    Custom hook for RabbitMQ operations
    """
    
    conn_name_attr = 'rabbitmq_conn_id'
    default_conn_name = 'rabbitmq_default'
    
    def __init__(self, rabbitmq_conn_id: str = default_conn_name):
        super().__init__()
        self.rabbitmq_conn_id = rabbitmq_conn_id
        self.connection = None
        self.channel = None
        
    def get_conn(self) -> pika.BlockingConnection:
        """Return rabbitmq connection."""
        if self.connection is None or self.connection.is_closed:
            conn = self.get_connection(self.rabbitmq_conn_id)
            
            credentials = pika.PlainCredentials(
                conn.login, 
                conn.password
            )
            
            extra_params = conn.extra_dejson
            
            parameters = pika.ConnectionParameters(
                host=conn.host,
                port=conn.port or 5672,
                virtual_host=extra_params.get('virtual_host', '/'),
                credentials=credentials,
                heartbeat=extra_params.get('heartbeat', 600),
                blocked_connection_timeout=extra_params.get('blocked_connection_timeout', 300)
            )
            
            self.connection = pika.BlockingConnection(parameters)
            
        return self.connection
    
    def get_channel(self):
        """Get or create channel."""
        if self.channel is None or self.channel.is_closed:
            self.channel = self.get_conn().channel()
        return self.channel
    
    def publish(
        self,
        exchange: str,
        routing_key: str,
        message: Any,
        exchange_type: str = 'direct',
        properties: Optional[Dict] = None,
        declare_exchange: bool = True
    ):
        """
        Publish message to RabbitMQ
        """
        channel = self.get_channel()
        
        if declare_exchange and exchange:
            channel.exchange_declare(
                exchange=exchange,
                exchange_type=exchange_type,
                durable=True
            )
        
        # Convert message to string if needed
        if not isinstance(message, str):
            if isinstance(message, dict):
                message = json.dumps(message)
            else:
                message = str(message)
        
        # Prepare properties
        basic_properties = pika.BasicProperties()
        if properties:
            for key, value in properties.items():
                setattr(basic_properties, key, value)
        
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=basic_properties
        )
        
        self.log.info(f"Message published to {exchange}/{routing_key}")
    
    def close(self):
        """Close connection."""
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()
            
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()