package com.example.study.consumer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.time.Duration

class OffsetCommitConsumer
private val logger = LoggerFactory.getLogger(OffsetCommitConsumer::class.java)

fun consumeSyncCommit() {
    val configs = createKafkaConfig()
    // auto commit off
    configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(arrayListOf(TOPIC_NAME))

    // 무한 루프를 돌면서 .. 계속 consume
    while (true) {
        // record 가져오기
        val records = consumer.poll(Duration.ofSeconds(1))

        // key : 파티션 정보 (TopicPartition)
        // value : 오프셋 정보 (OffsetAndMetadata)
        val currentOffset = HashMap<TopicPartition, OffsetAndMetadata>()

        records.forEach {
            logger.info("record : {}", it)

            val partition = TopicPartition(it.topic(), it.partition())
            currentOffset[partition] = OffsetAndMetadata(it.offset() + 1, null)

            // currentOffset 을 전달해야 명시적 commit 이 가능
            consumer.commitSync(currentOffset)
        }
    }
}

fun consumeAsyncCommit() {
    val configs = createKafkaConfig()
    // auto commit off
    configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(arrayListOf(TOPIC_NAME))

    // 무한 루프를 돌면서 .. 계속 consume
    while (true) {
        // record 가져오기
        val records = consumer.poll(Duration.ofSeconds(1))

        records.forEach {
            logger.info("record : {}", it)

            // 비동기 처리를 위한 callback 작성
            val callback = object: OffsetCommitCallback {
                override fun onComplete(offsets: MutableMap<TopicPartition, OffsetAndMetadata>, exception: Exception?) {
                    if (exception == null) {
                        logger.info("commit success")
                    } else {
                        logger.error("commit failed .. offsets: {}", offsets, exception)
                    }
                }
            }

            consumer.commitAsync(callback)
        }
    }
}