package com.example.study.consumer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Duration


class ConsumerWithRebalanceListener
private val logger = LoggerFactory.getLogger(ConsumerWithRebalanceListener::class.java)


private lateinit var consumer : KafkaConsumer<String, String>
private lateinit var currentOffset : HashMap<TopicPartition, OffsetAndMetadata>

fun consumeWithRebalanceListener() {
    val configs = createKafkaConfig()

    // auto commit off
    configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

    consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(arrayListOf(TOPIC_NAME), RebalanceLinstenr())

    // 무한 루프를 돌면서 .. 계속 consume
    while (true) {
        // record 가져오기
        val records = consumer.poll(Duration.ofSeconds(1))

        currentOffset = HashMap<TopicPartition, OffsetAndMetadata>()
        records.forEach {
            logger.info("record : {}", it)

            val partition = TopicPartition(it.topic(), it.partition())
            currentOffset[partition] = OffsetAndMetadata(it.offset() + 1, null)
            consumer.commitSync(currentOffset)
        }
    }
}

internal class RebalanceLinstenr : ConsumerRebalanceListener {
    // 리밸런스가 시작되기 전에 호출됨
    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        logger.warn("Partitions are revoked")
        // 리밸런스가 시작되기 전에 저장하고 있던 마지막 정보를 offset 을 커밋
        consumer.commitSync(currentOffset)
    }

    // 리밸런스가 끝난 뒤에 파티션 할당이 완료되면 호출됨
    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
        logger.warn("Partitions are assigned")
    }
}