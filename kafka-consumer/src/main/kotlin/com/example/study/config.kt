package com.example.study

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*


private const val BOOTSTRAP_SERVERS = "my-kafka:9092"
private const val GROUP_ID = "test-group"
const val TOPIC_NAME = "test"

fun createKafkaConfig() : Properties {
    val configs = Properties()
    // 카프카 접속 정보 설정
    configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
    // 컨슈머 그룹 아이디 지정
    configs[ConsumerConfig.GROUP_ID_CONFIG] = GROUP_ID
    // key, value 역직렬화 클래스 지정
    configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

    return configs
}