package com.example.study

import com.example.study.producer.*


// bin/kafka-console-consumer.sh --bootstrap-server my-kafka:9092 \
//    --topic test --from-beginning

fun main(args: Array<String>) {

//    produceSimple()
//    produceExactPartition()
//    produceWithCustomPartitioner()

//    produceSyncCheck()
    produceAsyncCheck()
}