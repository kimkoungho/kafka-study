package com.example.study

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*


private const val BOOTSTRAP_SERVERS = "my-kafka:9092"
const val TOPIC_NAME = "test"

fun createKafkaConfig() : Properties {
    val configs = Properties()
    // 카프카 접속 정보 설정
    configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
    // key, value 직렬화 클래스 지정
    configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

    return configs
}