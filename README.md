# mqtt-async-client-rs

An MQTT 3.1.1 client written in Rust, using async functions and tokio.

* Repository: <https://github.com/fluffysquirrels/mqtt-async-client-rs>
* Documentation: <https://docs.rs/mqtt-async-client>
* Cargo crate: <https://crates.io/crates/mqtt-async-client>
* CI builds on Travis CI: <https://travis-ci.com/fluffysquirrels/mqtt-async-client-rs>

Pull requests and Github issues welcome!

## To run automated tests

Simply run `cargo test`.

The integration tests require an MQTT broker to run against, see the
instructions in `${REPO}/tests/integration_test.rs`.

## Run the test command-line app

Run `cargo run --example mqttc` to print usage.

The test app requires an MQTT broker to run against, see the
instructions in `${REPO}/tests/integration_test.rs`.

Run `cargo run --example mqttc -- --host localhost publish topic payload`
to publish payload `payload` to topic `topic`.

Run `RUST_LOG="info" cargo run --example mqttc -- --host localhost subscribe topic`
to subscribe to topic `topic` and print any messages that are published to it.

## Changelog

### 0.3.1

* Add `--tls-client-crt-file` and `--tls-client-rsa-key-file` options to the example `mqttc`. Thanks to [marcelbuesing](https://github.com/marcelbuesing) for the PR!
* Add more explanation of how to configure TLS to the `ClientBuilder` docs.

### 0.3.0

* Add WebSocket support under Cargo feature "websocket".
* Switch `ClientBuilder` to use a URL instead of host and port. This was a breaking change to make it simple for consumers to switch protocol.

Thanks to [JarredAllen](https://github.com/JarredAllen) for the implementation!

### 0.2.0

* Update `tokio` dependency to v1.2.0. Thanks to [marcelbuesing](https://github.com/marcelbuesing) for the PR!

### 0.1.7

* Implement `Debug` for `Client` and `ClientOptions`
* Reduce dependencies for faster and less fiddly builds: `env_logger`
  and `structopt` are now dev-dependencies, `rustls` is now optional but
  included by default as part of the `tls` feature.

Thanks to [marcelbuesing](https://github.com/marcelbuesing) for the PRs!

### 0.1.6

* `Client` is `Send`.

### 0.1.5

* Correctly connect only once when automatic_connect is disabled.

### 0.1.4

* Missing ping responses should cause a disconnect even when keepalive > op timeout.

* Publish with retain flag.

### 0.1.3

* Added timeouts to disconnect, and publish when QoS=0.

### 0.1.2

* Enable automatic reconnects by default.

* This tracks subscriptions and replays them after reconnecting. No publish retries yet.
