//! One connection to the in-process server: what `fred`'s mock layer hands each command of one
//! client to.

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use fred::error::Error;
use fred::mocks::{MockCommand, Mocks};
use fred::types::Value;
use fred::types::config::Config;

use super::Server;
use super::args::{ok, server_error};

/// A client's connection: its commands reach the server one at a time, and a `MULTI` it sends
/// queues the commands after it until its `EXEC`, as a connection's transaction state does.
struct Connection {
    server: Arc<Server>,
    queued: Mutex<Option<Vec<MockCommand>>>,
}

impl Debug for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessConnection")
            .finish_non_exhaustive()
    }
}

/// `config` over a new connection to `server`.
pub(super) fn config(config: &Config, server: &Arc<Server>) -> Config {
    let mut config = config.clone();
    config.mocks = Some(Arc::new(Connection {
        server: Arc::clone(server),
        queued: Mutex::new(None),
    }));
    config
}

impl Mocks for Connection {
    fn process_command(&self, command: MockCommand) -> Result<Value, Error> {
        let mut queued = self.queued.lock().expect("in-process connection poisoned");
        let name = command.cmd.to_ascii_uppercase();
        match (name.as_str(), queued.as_mut()) {
            ("MULTI", Some(_)) => Err(server_error("ERR MULTI calls can not be nested")),
            ("MULTI", None) => {
                *queued = Some(Vec::new());
                Ok(ok())
            }
            ("EXEC", None) => Err(server_error("ERR EXEC without MULTI")),
            ("DISCARD", None) => Err(server_error("ERR DISCARD without MULTI")),
            ("EXEC", Some(_)) => {
                let commands = queued.take().unwrap_or_default();
                drop(queued);
                self.server.transaction(&commands)
            }
            ("DISCARD", Some(_)) => {
                *queued = None;
                Ok(ok())
            }
            (_, Some(commands)) => {
                commands.push(command);
                Ok(Value::Queued)
            }
            (_, None) => {
                drop(queued);
                self.server.apply(&command)
            }
        }
    }

    /// `fred`'s own transaction hands every command between its `MULTI` and its `EXEC` at once.
    fn process_transaction(&self, commands: Vec<MockCommand>) -> Result<Value, Error> {
        self.server.transaction(&commands)
    }
}
