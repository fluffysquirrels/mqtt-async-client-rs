#![deny(warnings)]

use mqtt_client::{
    client::{
        Client,
        Publish,
        QoS,
        Subscribe,
        SubscribeReturnCodes,
        SubscribeTopic,
    },
    Result,
};
use tokio;

#[test]
fn pub_and_sub() -> Result<()> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(hi2())
}

async fn pub_and_sub2() -> Result<()> {
    let mut c = client()?;
    c.connect().await?;

    // Subscribe to "a"
    let subopts = Subscribe::new(vec![
        SubscribeTopic {qos: QoS::AtMostOnce, topic_path: "a".to_owned() }
    ]);
    let subres = c.subscribe(subopts).await?;
    let any_failed = subres.any_failures()?;

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

fn client() -> Result<Client> {
    Client::builder()
        .set_host("localhost".to_owned())
        .build()
}
