use bson::{self, Bson, Document};
use futures_lite::stream::StreamExt;
use hostname;
use lapin::{
    message::Delivery, options::*, types::FieldTable, Channel, Connection, ConnectionProperties,
    Error as LapinError,
};
use lettre::{
    message::{header::ContentType, Attachment, MultiPart, SinglePart},
    transport::smtp::client::Tls,
    Message, SmtpTransport, Transport,
};
use num_cpus;
use serde::Deserialize;
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

    let mut queue_opts = QueueDeclareOptions::default();
    queue_opts.durable = true;
    channel
        .queue_declare(&queue_name, queue_opts, FieldTable::default())
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

#[derive(Deserialize)]
struct CustomAttachment {
    cid: String,
    inline: bool,
    content_type: String,
    data: Vec<u8>,
}

#[derive(Deserialize)]
struct EmailData {
    from: String,
    to: Vec<String>,
    bcc: Vec<String>,
    html: String,
    text: String,
    subject: String,
    reply_to: String,
    attachments: Vec<CustomAttachment>,
}

impl EmailData {
    fn from_delivery(delivery: &Delivery) -> Result<EmailData, bson::de::Error> {
        let document: Document = bson::from_slice(&delivery.data)?;
        let bson = Bson::Document(document);

        // Deserialize Bson into EmailData
        let email: EmailData = bson::from_bson(bson)?;

        Ok(email)
    }
}

pub async fn consume(delivery: Result<Delivery, LapinError>, channel: Channel) {
    match delivery {
        Ok(delivery) => {
            println!("Received message: {:?}", delivery);
            // send mail
            // Acknowledge the message

            let mailer = SmtpTransport::relay("host") // TODO
                // change
                // host
                .unwrap()
                .port(26)
                .tls(Tls::None)
                .build();

            let data = EmailData::from_delivery(&delivery);

            if let Ok(d) = data {
                let parts = MultiPart::mixed().singlepart(
                    SinglePart::builder()
                        .header(ContentType::TEXT_HTML)
                        .body(d.html),
                );

                for attachment in d.attachments {
                    match attachment.inline {
                        true => {
                            // attach inline
                            let part = Attachment::new_inline(attachment.cid).body(
                                attachment.data,
                                ContentType::parse(attachment.content_type.as_str()).unwrap(),
                            );

                            parts.clone().singlepart(part);
                        }
                        false => {
                            // dont attach inline
                            let part = Attachment::new(attachment.cid).body(
                                attachment.data,
                                ContentType::parse(attachment.content_type.as_str()).unwrap(),
                            );

                            parts.clone().singlepart(part);
                        }
                    }
                }

                let email = Message::builder()
                    .from(d.from.parse().unwrap())
                    .reply_to(d.reply_to.parse().unwrap())
                    .to(d.to.join(", ").parse().unwrap())
                    .bcc(d.bcc.join(", ").parse().unwrap())
                    .subject(d.subject);

                email.clone().multipart(parts).unwrap();
                let email = email.body(d.text).unwrap();

                let sent = mailer.send(&email);

                match sent {
                    Ok(_) => {
                        //ack
                        channel
                            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                            .await
                            .expect("Failed to ack message");
                    }
                    Err(_e) => {
                        // requeue
                        let mut nack_opts = BasicNackOptions::default();
                        nack_opts.requeue = true;
                        channel
                            .basic_nack(delivery.delivery_tag, nack_opts)
                            .await
                            .expect("Failed to nack message");
                    }
                }
            } else {
                // dead letter
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
