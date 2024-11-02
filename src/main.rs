use futures_lite::stream::StreamExt;
use hostname;
use lapin::{
    message::Delivery, options::*, types::FieldTable, Channel, Connection, ConnectionProperties,
    Error as LapinError,
};
use num_cpus;
use tokio_retry::{strategy::ExponentialBackoff, Retry};

const RMQ_URL: &str = "amqp://localhost:5672/%2f";

async fn create_connection(uri: &str) -> lapin::Result<Connection> {
    let retry_strategy = ExponentialBackoff::from_millis(10).take(50);

    Retry::spawn(retry_strategy, || async {
        println!("Attempting to connect to RabbitMQ...");
        Connection::connect(uri, ConnectionProperties::default()).await
    })
    .await
}

async fn handle_messages(channel: &lapin::Channel, core: usize) {
    // Example queue declaration
    let mut consumer_tag = "consumer-mailing-".to_string();
    let mut queue_name = "queue-mailing-".to_string();
    consumer_tag.push_str(core.to_string().as_str());
    queue_name.push_str(hostname::get().unwrap().to_str().unwrap());
    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Queue declaration failed");

    let mut consumer = channel
        .basic_consume(
            &queue_name,
            &consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to create consumer");

    println!("receiving messages from queue {}", &queue_name);

    while let Some(delivery) = consumer.next().await {
        consume(delivery, channel.clone()).await;
    }
}

pub async fn consume(delivery: Result<Delivery, LapinError>, channel: Channel) {
    match delivery {
        Ok(delivery) => {
            let data = std::str::from_utf8(&delivery.data);
            println!("Received message: {:?}", data);
            // send mail
            // Acknowledge the message
            if true {
                channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await
                    .expect("Failed to ack message");
            } else {
                let mut nack_opts = BasicNackOptions::default();
                nack_opts.requeue = false;
                channel
                    .basic_nack(delivery.delivery_tag, nack_opts)
                    .await
                    .expect("Failed to nack message");
            }
        }
        Err(error) => eprintln!("Error receiving message: {:?}", error),
    }
}

#[tokio::main]
async fn main() {
    let num_cores = num_cpus::get();

    let mut handles = Vec::new();
    for core in 0..num_cores {
        let handle = tokio::spawn(async move {
            loop {
                let conn = create_connection(&RMQ_URL).await;
                match conn {
                    Ok(conn) => {
                        let channel = conn
                            .create_channel()
                            .await
                            .expect("Failed to create channel");
                        handle_messages(&channel, core).await;
                    }
                    Err(e) => {
                        eprintln!("Connection failed: {}, retrying...", e);
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
