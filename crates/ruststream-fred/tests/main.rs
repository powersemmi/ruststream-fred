//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;

mod adopted_pool;
mod asyncapi;
mod block_order;
mod both_modes_fred;
mod claiming;
mod cluster_transactions;
mod conformance_fred;
mod conformance_suites;
mod connect_runtime;
mod deferred_retry;
mod envelopes;
mod framing;
mod fred_pool;
mod harness_manual;
mod in_process_fred;
mod integration_claiming;
mod integration_fred;
mod integration_pipeline;
mod integration_pubsub;
mod integration_redelivery;
mod pipeline_batch;
mod pipeline_bind;
mod pipeline_list;
mod pipeline_pubsub;
mod pipeline_stream;
mod preludes;
mod publish_options;
mod reconnect_live;
mod redelivery_per_form;
mod retry_declaration;
mod routes;
mod secrets;
mod testing_default_group;
mod testing_forms;
