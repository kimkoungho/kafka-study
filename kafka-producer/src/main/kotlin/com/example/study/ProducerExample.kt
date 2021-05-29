package com.example.study

import com.example.study.producer.*

// 토픽 체크
// bin/kafka-console-consumer.sh --bootstrap-server my-kafka:9092 --topic test --from-beginning

fun main(args: Array<String>) {

//    produceSimple()
    produceExactPartition()
//    produceWithCustomPartitioner()

//    produceSyncCheck()
//    produceAsyncCheck()
}