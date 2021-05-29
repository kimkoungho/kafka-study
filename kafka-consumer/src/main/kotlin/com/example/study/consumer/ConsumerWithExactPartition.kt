package com.example.study.consumer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class ConsumerWithExactPartition
private val logger = LoggerFactory.getLogger(ConsumerWithExactPartition::class.java)


const val PARTITION_NUMBER = 0
fun consumeWithExactPartition() {
    val configs = createKafkaConfig()

    val consumer = KafkaConsumer<String, String>(configs)

    // TopicPartition : 토픽, 파티션 정보 정의
    val topicPartition = TopicPartition(TOPIC_NAME, PARTITION_NUMBER)
    // 파티션 할당
    consumer.assign(Collections.singleton(topicPartition))

    while (true) {
        // record 가져오기
        val records = consumer.poll(Duration.ofSeconds(1))

        // key : 파티션 정보 (TopicPartition)
        // value : 오프셋 정보 (OffsetAndMetadata)
        val currentOffset = HashMap<TopicPartition, OffsetAndMetadata>()

        records.forEach {
            logger.info("record : {}", it)

        }
    }
}

fun checkPartition() {
    val configs = createKafkaConfig()

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(arrayListOf(TOPIC_NAME))

    val assignedTopicPartition = consumer.assignment()
}