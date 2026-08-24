# Getting started

The TiKV client is a Rust library (crate). To use this crate in your project, add the following dependencies to your `Cargo.toml`:

```toml
[dependencies]
tikv-client = "0.1"
tokio = { version = "1.5", features = ["full"] }
```

Note that you need to use Tokio. The TiKV client has an async API and therefore you need an async runtime in your program to use it. At the moment, Tokio is used internally in the client and so you must use Tokio in your code too. We plan to become more flexible in future versions.

The general flow of using the client crate is to create either a raw or transaction client object (which can be configured) then send commands using the client object, or use it to create transactions objects. In the latter case, the transaction is built up using various commands and then committed (or rolled back).

## Examples

To use the client in your program, use code like the following.

Raw mode:

```rust
use tikv_client::RawClient;

let client = RawClient::new(vec!["127.0.0.1:2379"], None).await?;
client.put("key".to_owned(), "value".to_owned()).await?;
let value = client.get("key".to_owned()).await?;
```

Transactional mode:

```rust
use tikv_client::TransactionClient;

let txn_client = TransactionClient::new(vec!["127.0.0.1:2379"], None).await?;
let mut txn = txn_client.begin_optimistic().await?;
txn.put("key".to_owned(), "value".to_owned()).await?;
let value = txn.get("key".to_owned()).await?;
txn.commit().await?;
```

To make an example which builds and runs,

```rust
use tikv_client::{TransactionClient, Error};

async fn run() -> Result<(), Error> {
    let txn_client = TransactionClient::new(vec!["127.0.0.1:2379"], None).await?;
    let mut txn = txn_client.begin_optimistic().await?;
    txn.put("key".to_owned(), "value".to_owned()).await?;
    let value = txn.get("key".to_owned()).await?;
    println!("value: {:?}", value);
    txn.commit().await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
```

For more examples, see the [examples](examples) directory.

## A TiKV cluster

To use the client, you'll need a TiKV instance to communicate with. In production, this should be a cluster of dedicated servers which are accessed via the network. To get started, you can run a TiKV 'cluster' on your local machine.

A TiKV cluster consists of TiKV nodes and PD nodes. For normal use, you need at least three of each; there is no maximum. For testing etc., you need at least one TiKV node
and one PD node. For more details, see the [TiKV docs](https://tikv.org/docs/dev/concepts/architecture/).

The easiest way to manage a TiKV cluster (locally or on multiple machines) is to use [TiUP](https://github.com/pingcap/tiup). To install it on your computer, use

```
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
```

then, to start a local TiKV 'cluster' for testing,

```
tiup playground nightly --mode tikv-slim
```

For more information about TiUP, see their [docs](https://docs.pingcap.com/tidb/stable/production-deployment-using-tiup).

You can also build and/or run TiKV and PD instances manually. See [this blog post](https://ncameron.org/blog/building-running-and-benchmarking-tikv-and-tidb/) for details, note that you don't need the TiDB nodes.
