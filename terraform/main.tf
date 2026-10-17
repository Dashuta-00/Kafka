terraform {
  required_providers {
    kafka = {
      source = "Mongey/kafka"
    }
  }
  required_version = ">= 0.13"
}

provider "kafka"{
  bootstrap_servers = ["kafka:29092"]  
  tls_enabled = false
}

resource "kafka_topic" "order_created" {
  name               = "order-created"
  replication_factor = 1
  partitions         = 3   
  
  config = {
    "retention.ms"   = "604800000"
    "cleanup.policy" = "delete"
  } 
}

resource "kafka_topic" "order_created_dlq" {
  name               = "order-created-dlq"
  replication_factor = 1
  partitions         = 1

  config = {
    "retention.ms"   = "1209600000" # 14 дней — сбойные храним дольше
    "cleanup.policy" = "delete"
  }
}


output "topics" {
  value = [
    kafka_topic.order_created.name,
    kafka_topic.order_created_dlq.name  
  ]
}