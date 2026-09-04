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
//! `Publish` names that form's policy value - a policy this broker pairs. They are compile-time
//! bounds rather than assertions: a prelude that drops an alias, or lets a policy take the
//! capability word, fails to compile here and nowhere else.

mod stream_prelude {
    use ruststream_fred::ConnectedRedisBroker;
    use ruststream_fred::stream::prelude::*;

    /// The handler-side word: still the broker capability trait, not a policy type.
    fn _capability<T: Publisher>() {}

    /// The mount site's other half: what it names is a policy this broker can pair.
    fn pairs<P: PublishPolicy<ConnectedRedisBroker>>() {}

    /// The mount-site word, in both spellings this form offers. A stream publisher buffers on the
    /// handle and owns transactions as it is, so the two name one policy.
    #[test]
    fn the_mount_site_word_names_this_forms_policy() {
        // A stream policy carries no options, so the value is the bare unit struct.
        let _: Publish = Publish;
        let _: TransactionalPublish = TransactionalPublish;
        pairs::<Publish>();
        pairs::<TransactionalPublish>();
    }
}

mod list_prelude {
    use ruststream_fred::ConnectedRedisBroker;
    use ruststream_fred::list::prelude::*;

    fn _capability<T: Publisher>() {}

    fn pairs<P: PublishPolicy<ConnectedRedisBroker>>() {}

    #[test]
    fn the_mount_site_word_names_this_forms_policy() {
        let _: Publish = Publish::default();
        pairs::<Publish>();
    }
}

mod pubsub_prelude {
    use ruststream_fred::ConnectedRedisBroker;
    use ruststream_fred::pubsub::prelude::*;

    fn _capability<T: Publisher>() {}

    fn pairs<P: PublishPolicy<ConnectedRedisBroker>>() {}

    #[test]
    fn the_mount_site_word_names_this_forms_policy() {
        let _: Publish = Publish::new().mode(PubSubMode::Sharded);
        pairs::<Publish>();
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
