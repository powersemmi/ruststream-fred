//! Every prelude of this crate must leave the core's own vocabulary intact.
//!
//! Each of them globs `ruststream::prelude::*`, and an explicit re-export beats that glob. A
//! publish policy exported under a core trait's name would therefore shadow the trait silently,
//! and the service that writes the bound gets `expected trait, found struct` pointing at its own
//! code. These probes fail to compile the moment a prelude takes one of those words back, which is
//! why they are compile-time bounds rather than assertions: there is nothing to observe at run
//! time.
//!
//! Each module also names its form's policy under the prefixed name, so a prelude that drops it
//! fails here too.

mod crate_prelude {
    use ruststream_fred::prelude::*;

    /// Resolves only while `Publish` names the core's slot capability trait.
    fn _core_trait<T: Publish>() {}

    fn _policies() {
        let _ = (
            RedisPublish,
            RedisListPublish::new(),
            RedisPubSubPublish::new(),
        );
    }
}

mod stream_prelude {
    use ruststream_fred::stream::prelude::*;

    fn _core_trait<T: Publish>() {}

    fn _policy() {
        let _ = TypedPublisher::new(RedisPublish).transactional();
    }
}

mod list_prelude {
    use ruststream_fred::list::prelude::*;

    fn _core_trait<T: Publish>() {}

    fn _policy() {
        let _ = TypedPublisher::new(RedisListPublish::new());
    }
}

mod pubsub_prelude {
    use ruststream_fred::pubsub::prelude::*;

    fn _core_trait<T: Publish>() {}

    fn _policy() {
        let _ = TypedPublisher::new(RedisPubSubPublish::new().mode(PubSubMode::Sharded));
    }
}
