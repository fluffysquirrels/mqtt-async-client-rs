# Release process

1. Push changes to [GitHub][github].
1. Increment version number in `Cargo.toml` (major version if breaking changes).
1. Check build locally with `bin/build_tc`. This runs several builds and unit tests, and will update the version number in `Cargo.lock`.
1. Check build locally with `bin/integration-tests`. This will run the integration tests, requires an MQTT server running on localhost, use `bin/mosquitto-test` in another terminal.
1. Update the changelog.
1. Commit the changes.
1. Add a git tag for the new version number and push it to [GitHub][github]:

    `git tag vX.Y.Z && git push --tags`

1. Check build on travis CI: <https://travis-ci.com/fluffysquirrels/mqtt-async-client-rs>
1. Publish with `bin/publish`.
1. Check new version appears on
   [![Crate](https://img.shields.io/crates/v/mqtt-async-client.svg)][crates]
   and
   [![Documentation](https://docs.rs/mqtt-async-client/badge.svg)][docs]

   [github]: https://github.com/fluffysquirrels/mqtt-async-client-rs
   [crates]: https://crates.io/crates/mqtt-async-client
   [docs]: https://docs.rs/mqtt-async-client
