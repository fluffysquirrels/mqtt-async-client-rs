#![deny(warnings)]

use log::{debug, error, info};
use mqtt_client::{
    client::{
        Client,
        Publish as PublishOpts,
        QoS,
        Subscribe as SubscribeOpts,
        SubscribeTopic,
    },
    Result,
};
use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
struct Args {
    #[structopt(subcommand)]
    cmd: Command,

    #[structopt(long)]
    username: Option<String>,

    #[structopt(long)]
    password: Option<String>,

    #[structopt(long)]
    host: String,

    #[structopt(long, default_value="1883")]
    port: u16,

    // TODO: TLS: CA cert to validate server, client cert.
}

#[derive(Clone, Debug, StructOpt)]
enum Command {
    Publish(Publish),
    Subscribe(Subscribe),
}

#[derive(Clone, Debug, StructOpt)]
struct Publish {
    topic: String,
    message: String,
    // TODO: Message retention.
    // TODO: QoS.
}

#[derive(Clone, Debug, StructOpt)]
struct Subscribe {
    topic: Vec<String>,
    // TODO: QoS.
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::from_args();
    debug!("Args: {:#?}", args);

    let res = match args.cmd {
        Command::Publish(ref sub_args) => publish(sub_args.clone(), args.clone()).await,
        Command::Subscribe(ref sub_args) => subscribe(sub_args.clone(), args).await,
    };
    if let Err(e) = res {
        error!("{}", e);
    }
}

async fn publish(pub_args: Publish, args: Args) -> Result<()> {
    let mut client = client_from_args(args)?;
    client.connect().await?;
    client.publish(PublishOpts::new(pub_args.topic.clone(), pub_args.message.as_bytes().to_vec()))
        .await?;
    info!("Published topic={}, message={}", pub_args.topic, pub_args.message);
    client.disconnect().await?;
    Ok(())
}

async fn subscribe(sub_args: Subscribe, args: Args) -> Result<()> {
    let mut client = client_from_args(args)?;
    client.connect().await?;
    let _subres = client.subscribe(SubscribeOpts::new(sub_args.topic.iter().map(|t|
        SubscribeTopic {qos: QoS::AtMostOnce, topic_path: t.clone() }
    ).collect())).await?;
    // TODO: Check subres.
    loop {
        let r = client.read().await?;
        info!("Read r={:?}", r);
    }
}

fn client_from_args(args: Args) -> Result<Client> {
    Client::builder()
        .host(args.host)
        .port(args.port)
        .username(args.username)
        .password(args.password)
        .build()
}
