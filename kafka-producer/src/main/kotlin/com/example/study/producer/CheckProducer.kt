package com.example.study.producer

import com.example.study.TOPIC_NAME
import com.example.study.createKafkaConfig
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.lang.Exception


// 브로커에 데이터가 적재되었는지 검사 - sync
fun produceSyncCheck() {
    val configs = createKafkaConfig()

    // 프로듀서 생성
    val producer = KafkaProducer<String, String>(configs)

    val messageKey = "syncCallbackKey"
    val messageValue = "syncCallbackMsg"
    // 전송 단위 record 지정
    val record = ProducerRecord(TOPIC_NAME, messageKey, messageValue)

    // 전송 요청
    val callbackFuture = producer.send(record)
    val metadata = callbackFuture.get()


    // send 는 레코드를 모아서 배치형태로 전송하기 때문에 flush 로 버퍼를 비우도록 .. 전송을 의미함
    producer.flush()
    producer.close()
}

// 브로커에 데이터가 적재되었는지 검사 - async
internal class ProducerCallback : Callback {
    private val logger = LoggerFactory.getLogger(this.javaClass)

    override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
        if (exception != null) {
            logger.error(exception.message, exception)
        } else {
            logger.info("ProducerCallback metadata: {}", metadata.toString())
        }
    }
}

fun produceAsyncCheck() {
    val configs = createKafkaConfig()

    // 프로듀서 생성
    val producer = KafkaProducer<String, String>(configs)

    val messageKey = "asyncCallbackKey"
    val messageValue = "asyncCallbackMsg"
    // 전송 단위 record 지정
    val record = ProducerRecord(TOPIC_NAME, messageKey, messageValue)

    producer.send(record, ProducerCallback())

    // send 는 레코드를 모아서 배치형태로 전송하기 때문에 flush 로 버퍼를 비우도록 .. 전송을 의미함
    producer.flush()
    producer.close()
}