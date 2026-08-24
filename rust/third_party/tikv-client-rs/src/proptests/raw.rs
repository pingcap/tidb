// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use super::{arb_batch, pd_addrs};
use crate::{raw::Client, Config, KvPair, Value};
use futures::executor::block_on;
use proptest::{arbitrary::any, proptest};
use std::collections::HashSet;

proptest! {
    /// Test single point (put, get, delete) operations on keys.
    #[test]
    #[cfg_attr(not(feature = "integration-tests"), ignore)]
    fn point(
        pair in any::<KvPair>(),
    ) {
        let client = block_on(Client::new(Config::new(pd_addrs()))).unwrap();

        block_on(
            client.put(pair.key().clone(), pair.value().clone())
        ).unwrap();

        let out_value = block_on(
            client.get(pair.key().clone())
        ).unwrap();

        assert_eq!(Some(Value::from(pair.value().clone())), out_value);

        block_on(
            client.delete(pair.key().clone())
        ).unwrap();
    }
}

proptest! {
    /// Test batch (put, get, delete) operations on keys.
    #[test]
    #[cfg_attr(not(feature = "integration-tests"), ignore)]
    fn batch(
        kvs in arb_batch(any::<KvPair>(), None),
    ) {
        let client = block_on(Client::new(Config::new(pd_addrs()))).unwrap();
        let keys = kvs.iter().map(|kv| kv.key()).cloned().collect::<Vec<_>>();
        let key_set = keys.iter().collect::<HashSet<_>>();

        // skip if there are duplicated keys
        if keys.len() == key_set.len() {
            block_on(
                client.batch_put(kvs.clone())
            ).unwrap();

            let out_value = block_on(
                client.batch_get_pairs(keys.clone())
            ).unwrap();
            assert_eq!(kvs, out_value);

            block_on(
                client.batch_delete(keys.clone())
            ).unwrap();
        }
    }
}
