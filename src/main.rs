use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Message};

use std::time::Duration;
use std::str::from_utf8;
use std::env::args;

fn main() {
    let mut arg = args();
    if arg.len() != 5 {
        println!("Usage : demo-kafka <brokers> <topic> <key> <message>\nDefault may be 127.0.0.1:9092, this programm do not create topic! (check auto.create.topics.enable)");
    } else {
        arg.next();
        let brokers = arg.next().unwrap();
        let topic = arg.next().unwrap();
        let key = arg.next().unwrap();
        let payload = arg.next().unwrap();
        produce(&brokers, &topic, &key, &payload);
        consume(&brokers, &topic, "1");
    }
}

//Produce one message, assuming topic is created
fn produce(brokers: &str, topic: &str, key: &str, payload: &str) {
    println!("Sending to \"{}\", topic is \"{}\"", brokers, topic);
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Producer creation failed");

    producer.send(BaseRecord::to(topic)
        .payload(payload)
        .key(key)
        ).expect("Failed to enqueue");

    for _ in 0..10 {
        producer.poll(Duration::from_millis(100));
    }   
    producer.flush(Duration::from_secs(1));
    println!("Key: \"{}\", Payload: \"{}\"", key, payload);
}

//Consume and display one message on one topic
fn consume(brokers: &str, topic: &str, group_id: &str) {
    println!("Receiving on \"{}\", topic is \"{}\"", brokers, topic);
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[topic])
        .expect("Can't subscribe to specified topics");

    let message = consumer.poll(None).unwrap();
    match message {
        Err(e) => println!("Kafka error: {}", e),
        Ok(msg) => { let m = msg.detach();
                     println!("Key: \"{}\", Payload: \"{}\"",
                     from_utf8( m.key().unwrap_or(b"None") ).unwrap(),
                     from_utf8( m.payload().unwrap_or(b"None") ).unwrap() )
                   },
    }
}

