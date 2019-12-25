#![deny(warnings)]

use futures_util::{
    stream::{
        futures_unordered::FuturesUnordered,
        StreamExt,
    },
};
use log::{debug, error, info};
use mqtt_client::{
    client::{
        Client,
        Publish as PublishOpts,
        QoS,
        Subscribe as SubscribeOpts,
        SubscribeReturnCodes,
        SubscribeTopic,
    },
    Error,
    Result,
};
use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
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

    #[structopt(long)]
    client_id: Option<String>,
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

    #[structopt(long,
                possible_values(&["0", "1", "2"]),
                default_value("0"))]
    qos: u8,

    #[structopt(long,
                default_value("1"))]
    repeats: u16,
    // TODO: Message retention.
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
        error!("{:?}", e);
    }
}

async fn publish(pub_args: Publish, args: Args) -> Result<()> {
    let mut client = client_from_args(args)?;
    client.connect().await?;
    let mut p = PublishOpts::new(pub_args.topic.clone(), pub_args.message.as_bytes().to_vec());
    p.set_qos(match pub_args.qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => panic!("Not reached"),
    });
    let futs = (0..(pub_args.repeats)).map(|_| {
        client.publish(&p)
    });
    let futs: FuturesUnordered<_> = futs.collect();
    futs.collect::<Vec<_>>().await;
    info!("Published topic={}, message={}", pub_args.topic, pub_args.message);
    client.disconnect().await?;
    Ok(())
}

async fn subscribe(sub_args: Subscribe, args: Args) -> Result<()> {
    let mut client = client_from_args(args)?;
    client.connect().await?;
    let subopts = SubscribeOpts::new(sub_args.topic.iter().map(|t|
        SubscribeTopic {qos: QoS::AtMostOnce, topic_path: t.clone() }
    ).collect());
    let subres = client.subscribe(subopts).await?;
    let any_failed = subres.return_codes().iter().any(|rc| *rc == SubscribeReturnCodes::Failure);
    if any_failed {
        return Err(format!("Some subscribes failed: {:#?}", subres.return_codes()).into());
    }
    loop {
        let r = client.read_published().await;
        info!("Read r={:?}", r);
        if let Err(Error::Disconnected) = r {
            return Err(Error::Disconnected);
        }
    }
}

fn client_from_args(args: Args) -> Result<Client> {
    Client::builder()
        .set_host(args.host)
        .set_port(args.port)
        .set_username(args.username)
        .set_password(args.password.map(|s| s.as_bytes().to_vec()))
        .set_client_id(args.client_id)
        .build()
}
