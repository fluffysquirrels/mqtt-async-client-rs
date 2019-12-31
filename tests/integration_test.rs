//! Some integration tests that require an MQTT 3.1.1 broker listening
//! on localhost:1833.

#![deny(warnings)]

use mqtt_client::{
    client::{
        Client,
        Publish,
        QoS,
        Subscribe,
        SubscribeTopic,
        Unsubscribe,
        UnsubscribeTopic,
    },
    Result,
};
use tokio::{
    self,
    time::{
        Duration,
        timeout,
    },
};

#[test]
fn pub_and_sub() -> Result<()> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(pub_and_sub2())
}

async fn pub_and_sub2() -> Result<()> {
    let mut c = client()?;
    c.connect().await?;

    // Subscribe to "a"
    let subopts = Subscribe::new(vec![
        SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "a".to_owned() }
    ]);
    let subres = c.subscribe(subopts).await?;
    subres.any_failures()?;

    // Publish to "a"
    let mut p = Publish::new("a".to_owned(), "x".as_bytes().to_vec());
    p.set_qos(QoS::AtMostOnce);
    c.publish(&p).await?;

    // Read from "a".
    let r = c.read_subscriptions().await?;
    assert_eq!(r.topic(), "a");
    assert_eq!(r.payload(), b"x");
    Ok(())
}

#[test]
fn unsubscribe() -> Result<()> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(unsubscribe2())
}

async fn unsubscribe2() -> Result<()> {
    let mut c = client()?;
    c.connect().await?;

    // Subscribe to "unsub_test"
    let subopts = Subscribe::new(vec![
        SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "unsub_test".to_owned() }
    ]);
    let subres = c.subscribe(subopts).await?;
    subres.any_failures()?;

    // Unsubscribe from "unsub_test"
    c.unsubscribe(Unsubscribe::new(vec![
        UnsubscribeTopic::new("unsub_test".to_owned()),
    ])).await?;

    // Publish to "a"
    let mut p = Publish::new("unsub_test".to_owned(), "x".as_bytes().to_vec());
    p.set_qos(QoS::AtMostOnce);
    c.publish(&p).await?;

    // Read from "a" and timeout.
    let r = timeout(Duration::from_secs(3), c.read_subscriptions()).await;
    assert!(r.is_err());
    Ok(())
}

fn client() -> Result<Client> {
    Client::builder()
        .set_host("localhost".to_owned())
        .build()
}
