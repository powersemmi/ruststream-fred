//! The two vocabularies this crate's preludes keep apart.
//!
//! A handler body imports `ruststream::prelude::*` and bounds an injected slot with the broker
//! capability trait it needs - `Publisher`, `TransactionalPublisher`, `OwnedTransactions`,
//! `RequestReply`. A routes file globs one mode prelude instead and names that form's publish
//! policy by its mount-site word, the same word on every form, so moving a handler between forms
//! changes the descriptor and not the mount.
//!
//! The two must not share names, which is what these probes pin. Through each mode prelude,
//! `Publisher` still resolves to the broker capability trait a handler would bound with, and
//! `Publish` names that form's policy value - a policy this broker pairs, against the real server
//! and against the in-process stand-in alike, so a routes file has one spelling for both. They are
//! compile-time bounds rather than assertions: a prelude that drops an alias, lets a policy take
//! the capability word, or leaves a form pairing on only one of the two brokers, fails to compile
//! here and nowhere else.

mod stream_prelude {
    use ruststream_fred::ConnectedRedisBroker;
    use ruststream_fred::stream::prelude::*;

    /// The handler-side word: still the broker capability trait, not a policy type.
    fn _capability<T: Publisher>() {}

    /// The mount site's other half: what it names is a policy this broker can pair.
    fn pairs<P: PublishPolicy<ConnectedRedisBroker>>() {}

    /// The same, against the in-process stand-in: one policy, two brokers.
    #[cfg(feature = "testing")]
    fn pairs_in_process<P: PublishPolicy<ruststream_fred::testing::ConnectedRedisTestBroker>>() {}

    /// The mount-site word, in both spellings this form offers. A stream publisher buffers on the
    /// handle and owns transactions as it is, so the two name one policy.
    #[test]
    fn the_mount_site_word_names_this_forms_policy() {
        // A stream policy carries no options, so the value is the bare unit struct.
        let _: Publish = Publish;
        let _: TransactionalPublish = TransactionalPublish;
        pairs::<Publish>();
        pairs::<TransactionalPublish>();
        #[cfg(feature = "testing")]
        pairs_in_process::<Publish>();
    }
}

mod list_prelude {
    use ruststream_fred::ConnectedRedisBroker;
    use ruststream_fred::list::prelude::*;

    fn _capability<T: Publisher>() {}

    fn pairs<P: PublishPolicy<ConnectedRedisBroker>>() {}

    #[cfg(feature = "testing")]
    fn pairs_in_process<P: PublishPolicy<ruststream_fred::testing::ConnectedRedisTestBroker>>() {}

    #[test]
    fn the_mount_site_word_names_this_forms_policy() {
        let _: Publish = Publish::default();
        pairs::<Publish>();
        #[cfg(feature = "testing")]
        pairs_in_process::<Publish>();
    }
}

mod pubsub_prelude {
    use ruststream_fred::ConnectedRedisBroker;
    use ruststream_fred::pubsub::prelude::*;

    fn _capability<T: Publisher>() {}

    fn pairs<P: PublishPolicy<ConnectedRedisBroker>>() {}

    #[cfg(feature = "testing")]
    fn pairs_in_process<P: PublishPolicy<ruststream_fred::testing::ConnectedRedisTestBroker>>() {}

    #[test]
    fn the_mount_site_word_names_this_forms_policy() {
        let _: Publish = Publish::new().mode(PubSubMode::Sharded);
        pairs::<Publish>();
        #[cfg(feature = "testing")]
        pairs_in_process::<Publish>();
    }
}

/// The crate prelude spans all three forms, so one mount-site word would name three colliding
/// types: it carries the prefixed names and the form modules instead.
mod crate_prelude {
    use ruststream_fred::ConnectedRedisBroker;
    use ruststream_fred::prelude::*;

    fn _capability<T: Publisher>() {}

    fn pairs<P: PublishPolicy<ConnectedRedisBroker>>() {}

    #[test]
    fn the_prefixed_names_serve_a_file_that_mixes_forms() {
        let _ = (
            RedisPublish,
            RedisListPublish::default(),
            RedisPubSubPublish::default(),
        );
        // Reached through the form modules, which is how a mixed file writes them.
        pairs::<stream::Publish>();
        pairs::<pubsub::Publish>();
    }
}

/// The stand-in must not be more capable than the transport it stands in for.
///
/// A handler bounds an injected slot with the capability it needs, and the bound is checked against
/// whatever the mounted policy pairs into. If the in-process publisher carried a capability the
/// real one lacks, the slot would compile under the harness and fail on the production build: the
/// failure lands after the tests were believed, which is the worst direction for a stand-in to be
/// wrong in. So each form pairs into a publisher with the same surface on both brokers.
///
/// The positive half is bounded directly below. The negative half - that the list and Pub/Sub forms
/// offer *no* transaction capability in process - cannot be written as a bound on stable, and this
/// repo has no trybuild machinery to hold a compile-fail case (the three-toolchain matrix would
/// need per-version expected output). It is pinned instead by identity: `pairs_into` names the
/// exact publisher each policy resolves to, so pointing a form back at the transactional stand-in
/// fails here, and `RedisTestPlainPublisher`'s only trait impls live in one file next to its `why`.
#[cfg(feature = "testing")]
mod capability_parity {
    use ruststream::{
        ConnectedBroker, OwnedTransactions, PublishPolicy, Publisher, TransactionalPublisher,
    };
    use ruststream_fred::testing::{
        ConnectedRedisTestBroker, RedisTestPlainPublisher, RedisTestPublisher,
    };
    use ruststream_fred::{
        ConnectedRedisBroker, RedisListPublish, RedisListPublisher, RedisPubSubPublish,
        RedisPubSubPublisher, RedisPublish, RedisPublisher,
    };

    /// The live publisher policy `P` pairs into against broker `B`.
    type Live<P, B> = <P as PublishPolicy<B>>::Live;

    /// The surface every publisher has.
    fn publishes<T: Publisher>() {}

    /// The surface only a stream publisher has, in both transaction kinds.
    fn transacts<T: TransactionalPublisher + OwnedTransactions>() {}

    /// Pins which publisher a policy resolves to, so a widened stand-in is caught here.
    fn pairs_into<P, B, Expected>()
    where
        B: ConnectedBroker,
        P: PublishPolicy<B, Live = Expected>,
    {
    }

    #[test]
    fn the_stream_form_transacts_on_both_brokers() {
        transacts::<Live<RedisPublish, ConnectedRedisBroker>>();
        transacts::<Live<RedisPublish, ConnectedRedisTestBroker>>();
        pairs_into::<RedisPublish, ConnectedRedisBroker, RedisPublisher>();
        pairs_into::<RedisPublish, ConnectedRedisTestBroker, RedisTestPublisher>();
    }

    #[test]
    fn the_list_and_pubsub_forms_only_publish_on_both_brokers() {
        publishes::<Live<RedisListPublish, ConnectedRedisBroker>>();
        publishes::<Live<RedisListPublish, ConnectedRedisTestBroker>>();
        publishes::<Live<RedisPubSubPublish, ConnectedRedisBroker>>();
        publishes::<Live<RedisPubSubPublish, ConnectedRedisTestBroker>>();

        pairs_into::<RedisListPublish, ConnectedRedisBroker, RedisListPublisher>();
        pairs_into::<RedisListPublish, ConnectedRedisTestBroker, RedisTestPlainPublisher>();
        pairs_into::<RedisPubSubPublish, ConnectedRedisBroker, RedisPubSubPublisher>();
        pairs_into::<RedisPubSubPublish, ConnectedRedisTestBroker, RedisTestPlainPublisher>();
    }
}
