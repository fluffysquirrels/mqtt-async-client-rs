# Release process

1. Push changes to [GitHub][github].
1. Increment version number in Cargo.toml (major version if breaking changes).
1. Check build locally with `cargo test`.
1. Update the changelog.
1. Add a git tag for the new version number and push it to [GitHub][github]:

    `git tag vX.Y.Z && git push --tags`

1. Publish with `bin/publish`.
1. Check new version appears on
   [![Crate](https://img.shields.io/crates/v/mqtt-async-client.svg)][crates]
   and
   [![Documentation](https://docs.rs/mqtt-async-client/badge.svg)][docs]

   [github]: https://github.com/fluffysquirrels/mqtt-async-client-rs
   [crates]: https://crates.io/crates/mqtt-async-client
   [docs]: https://docs.rs/mqtt-async-client
