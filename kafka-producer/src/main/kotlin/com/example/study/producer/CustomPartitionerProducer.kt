package com.example.study.producer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.utils.Utils


internal class CustomPartitioner : Partitioner {
    override fun configure(configs: MutableMap<String, *>?) {}

    override fun close() {}

    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster
    ): Int {
        // 메시지 키가 없는경우 throw
        if (keyBytes == null) {
            throw InvalidRecordException("Need message key")
        }

        // 판교는 1번 파티션으로 하드코딩
        if ("Pangyo".equals(key)) {
            return 0
        }

        // 파티션 크기로 제산법
        val partitions = cluster.partitionsForTopic(topic)
        return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions.size
    }
}

// 커스텀 파티셔너 이용
fun produceWithCustomPartitioner() {
    val configs = createKafkaConfig()
    // 파티셔너 지정
    configs[ProducerConfig.PARTITIONER_CLASS_CONFIG] = CustomPartitioner::class.java

    // 프로듀서 생성
    val producer = KafkaProducer<String, String>(configs)

    val recordPangyo = ProducerRecord(TOPIC_NAME, "Pangyo", "23")
    val recordSeoul = ProducerRecord(TOPIC_NAME, "Seoul", "10")

    producer.send(recordPangyo)
    producer.send(recordSeoul)
    producer.flush()
    producer.close()
}