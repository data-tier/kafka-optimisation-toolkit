/*
 * Licensed under the Apache License, Version 2.0. See LICENSE in the repository root.
 */
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.Future;

/**
 * kafka-producer-perf-test with uniformly random payload bytes.
 *
 * <p>The stock tool fills --record-size payloads with random uppercase letters
 * (ProducerPerformance.generateRandomPayload in 4.3), which compress well, and
 * --payload-file only accepts UTF-8 text. This class reuses the upstream tool
 * unchanged (argument parsing, throttling, latency statistics, the summary line
 * and --print-metrics) and only overwrites each payload with random bytes just
 * before it is handed to KafkaProducer.send.
 *
 * <p>It lives in org.apache.kafka.tools because ProducerPerformance.start and
 * createKafkaProducer are package-private. Overwriting the array is safe for the
 * same reason the upstream tool can reuse it: send() serializes the value and
 * copies it into the record batch before it returns.
 */
public class RandomBytesProducerPerformance extends ProducerPerformance {

    // Fixed seed so every run sends the same byte sequence, like the upstream tool.
    private final SplittableRandom random = new SplittableRandom(0);

    public static void main(String[] args) throws Exception {
        if (!Arrays.asList(args).contains("--record-size")) {
            System.err.println("RandomBytesProducerPerformance needs --record-size. "
                + "It overwrites the reused --record-size buffer, so --payload-file and "
                + "--payload-monotonic are not supported.");
            System.exit(1);
        }
        new RandomBytesProducerPerformance().start(args);
    }

    @Override
    KafkaProducer<byte[], byte[]> createKafkaProducer(Properties props) {
        return new KafkaProducer<>(props) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                random.nextBytes(record.value());
                return super.send(record, callback);
            }
        };
    }
}
