package com.example.study.producer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

// 파티션 번호지정 하기
fun produceExactPartition() {
    val configs = createKafkaConfig()
    // 프로듀서 생성
    val producer = KafkaProducer<String, String>(configs)

    // 파티션 번호 지정
    val partitionNo = 0;
    val record = ProducerRecord(TOPIC_NAME, partitionNo, "key", "value")
    producer.send(record)

    producer.flush()
    producer.close()
}