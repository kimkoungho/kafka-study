package com.example.study.consumer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.time.Duration

class ShutdownHookConsumer
private val logger = LoggerFactory.getLogger(ShutdownHookConsumer::class.java)

fun consumeWithShutdownHook() {
    val configs = createKafkaConfig()
    // auto commit off
    configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(arrayListOf(TOPIC_NAME))

    val shutdownThread = Thread {
        logger.info("Shutdown hook")
        consumer.wakeup()
    }
    Runtime.getRuntime().addShutdownHook(shutdownThread)

    try {
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
    } catch (e : WakeupException) {
        // poll 요청을 하다가 wakeup() 이 호출되면 WakeupException 발생
        logger.warn("Wakeup consumer")
        // 리소스 종료 처리
    } finally {
        // 카프카 클러스터에 알리기
        consumer.close()
    }
}
