use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Message};
use rdkafka::topic_partition_list::Offset;

use std::time::Duration;
use std::thread;
use std::str::from_utf8;
use std::env::args;

fn main() {
    let mut arg = args();
    arg.next();
    println!("{}",arg.len());
    if arg.len() != 4  && arg.len() != 2 {
        println!("Usage:\nproducer: demo-kafka <brokers> <topic> <key> <payload>\nconsumer: demo-kafka <brokers> <topic>\nDefault brockers 127.0.0.1:9092, this programm do not create topic! (check auto.create.topics.enable)");
    } else {
    match arg.len() {
        2 => { let brokers = arg.next().unwrap();
               let topic = arg.next().unwrap();
               consume(&brokers, &topic, "1") },
        4 => { let brokers = arg.next().unwrap();
               let topic = arg.next().unwrap();
               let key = arg.next().unwrap();
               let payload = arg.next().unwrap();
               produce(&brokers, &topic, &key, &payload) },
        _ => ()} //this will never happen
       
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

    // Poll at regular intervals to process all the asynchronous delivery events.
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

    for message in consumer.iter() {
        match message {
            Err(e) => println!("Kafka error: {}", e),
            Ok(msg) => { let m = msg.detach();
                         println!("Key: \"{}\", Payload: \"{}\"",
                         from_utf8( m.key().unwrap_or(b"None") ).unwrap(),
                         from_utf8( m.payload().unwrap_or(b"None") ).unwrap() )
                       },
    }   }
}

