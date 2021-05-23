package com.example.study.consumer


import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration


class SimpleConsumer
private val logger = LoggerFactory.getLogger(SimpleConsumer::class.java)

// key, value produce 예제
fun consumeSimple() {
    val configs = createKafkaConfig()

    // 컨슈머 생성
    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(arrayListOf(TOPIC_NAME))

    // 무한 루프를 돌면서 .. 계속 consume
    while (true) {
        // record 가져오기
        val records = consumer.poll(Duration.ofSeconds(1))

        records.forEach {
            logger.info("record : {}", it)
        }

    }
}

