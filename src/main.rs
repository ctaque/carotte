use bson::{self, Bson, Document};
use futures_lite::stream::StreamExt;
use hostname;
use lapin::{
    message::Delivery, options::*, types::FieldTable, Channel, Connection, ConnectionProperties,
    Error as LapinError,
};
use lettre::{
    address::AddressError,
    message::{header::ContentType, Attachment, Mailbox, MultiPart, SinglePart},
    transport::smtp::client::Tls,
    Address, Message, SmtpTransport, Transport,
};
use num_cpus;
use serde::Deserialize;
use tokio_retry::{strategy::ExponentialBackoff, Retry};

/*
 * TODO
 * - logging
 * - smtp conf (host)
 * - consume DLQ
 *
 */

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
        let consume_output = consume(delivery, channel.clone()).await;

        match consume_output {
            Ok(del) => {
                let _ = channel
                    .basic_ack(del.delivery_tag, BasicAckOptions::default())
                    .await;
            }
            Err(e) => {
                match e.delivery {
                    Some(del) => {
                        match e.action {
                            MsgAction::NackRequeue => {
                                // requeue
                                let mut nack_opts = BasicNackOptions::default();
                                nack_opts.requeue = true;
                                let _ = channel.basic_nack(del.delivery_tag, nack_opts).await;
                            }
                            MsgAction::DeadLetter => {
                                let mut nack_opts = BasicNackOptions::default();
                                nack_opts.requeue = false;
                                let _ = channel.basic_nack(del.delivery_tag, nack_opts).await;
                            }
                            MsgAction::NoOp => {
                                //
                            }
                        }
                    }
                    None => {
                        println!("Error message: {:#?}", e.message);
                    }
                }
            }
        }
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

enum MsgAction {
    NackRequeue,
    DeadLetter,
    NoOp,
}

pub struct CustomError {
    message: String,
    delivery: Option<Delivery>,
    action: MsgAction,
}

pub async fn consume(
    delivery: Result<Delivery, LapinError>,
    _channel: Channel,
) -> Result<Delivery, CustomError> {
    match delivery {
        Ok(delivery) => {
            println!("Received message: {:?}", delivery);
            // send mail
            // Acknowledge the message

            let mailer = SmtpTransport::relay("host")
                .unwrap()
                .port(26)
                .tls(Tls::None)
                .build();

            let data = EmailData::from_delivery(&delivery);

            match data {
                Ok(d) => {
                    let parts = MultiPart::mixed().singlepart(
                        SinglePart::builder()
                            .header(ContentType::TEXT_HTML)
                            .body(d.html),
                    );

                    for attachment in d.attachments {
                        let content_type_res = ContentType::parse(attachment.content_type.as_str());
                        if let Ok(content_type) = content_type_res {
                            match attachment.inline {
                                true => {
                                    // attach inline
                                    let part = Attachment::new_inline(attachment.cid)
                                        .body(attachment.data, content_type);

                                    parts.clone().singlepart(part);
                                }
                                false => {
                                    // dont attach inline
                                    let part = Attachment::new(attachment.cid)
                                        .body(attachment.data, content_type);

                                    parts.clone().singlepart(part);
                                }
                            }
                        } else {
                            return Err(CustomError {
                                message: format!("Could not parse attachment Content Type"),
                                delivery: Some(delivery),
                                action: MsgAction::DeadLetter,
                            });
                        }
                    }

                    let from: Result<Address, AddressError> = d.from.parse();
                    let reply_to: Result<Address, AddressError> = d.reply_to.parse();
                    let to: Result<Address, AddressError> = d.to.join(", ").parse();
                    let bcc: Result<Address, AddressError> = d.bcc.join(", ").parse();

                    if from.is_err() {
                        return Err(CustomError {
                            message: format!("Could not parse Mail From address"),
                            delivery: Some(delivery),
                            action: MsgAction::DeadLetter,
                        });
                    }

                    if reply_to.is_err() {
                        return Err(CustomError {
                            message: format!("Could not parse Reply To address"),
                            delivery: Some(delivery),
                            action: MsgAction::DeadLetter,
                        });
                    }

                    if to.is_err() {
                        return Err(CustomError {
                            message: format!("Could not parse Mail To address"),
                            delivery: Some(delivery),
                            action: MsgAction::DeadLetter,
                        });
                    }

                    if bcc.is_err() {
                        return Err(CustomError {
                            message: format!("Could not parse Bcc address"),
                            delivery: Some(delivery),
                            action: MsgAction::DeadLetter,
                        });
                    }

                    let email = Message::builder()
                        .from(from.unwrap().into())
                        .reply_to(reply_to.unwrap().into())
                        .to(to.unwrap().into())
                        .bcc(bcc.unwrap().into())
                        .subject(d.subject);

                    let _ = email.clone().multipart(parts);

                    let email_body_res = email.body(d.text);

                    let sent_res = match email_body_res {
                        Ok(email) => mailer.send(&email),
                        Err(e) => {
                            return Err(CustomError {
                                message: format!("Could not set email body: {}", e.to_string()),
                                delivery: Some(delivery),
                                action: MsgAction::NackRequeue,
                            });
                        }
                    };

                    match sent_res {
                        Ok(_) => {
                            //ack
                            Ok(delivery)
                        }
                        Err(e) => Err(CustomError {
                            message: format!("Could not send mail {:#?}", e.to_string()),
                            delivery: Some(delivery),
                            action: MsgAction::NackRequeue,
                        }),
                    }
                }
                Err(e) => Err(CustomError {
                    message: format!("Could not parse message data {:#?}", e),
                    delivery: Some(delivery),
                    action: MsgAction::DeadLetter,
                }),
            }
        }
        Err(error) => Err(CustomError {
            message: format!("Error receiving message: {:#?}", error.to_string()),
            delivery: None,
            action: MsgAction::NoOp,
        }),
    }
}

async fn dead_letter(delivery: &Delivery, channel: &Channel) {
    let mut nack_opts = BasicNackOptions::default();
    nack_opts.requeue = false;
    channel
        .basic_nack(delivery.delivery_tag, nack_opts)
        .await
        .expect("Failed to nack message");
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
