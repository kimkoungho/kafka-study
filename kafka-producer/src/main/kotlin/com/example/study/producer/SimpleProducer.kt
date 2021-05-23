package com.example.study.producer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord


// key, value produce 예제
fun produceSimple() {
    val configs = createKafkaConfig()

    // 프로듀서 생성
    val producer = KafkaProducer<String, String>(configs)

    val messageKey = "testKey"
    val messageValue = "testMessage"
    // 전송 단위 record 지정
    val record = ProducerRecord(TOPIC_NAME, messageKey, messageValue)
    // 전송 요청
    producer.send(record)

    // send 는 레코드를 모아서 배치형태로 전송하기 때문에 flush 로 버퍼를 비우도록 .. 전송을 의미함
    producer.flush()
    producer.close()
}

