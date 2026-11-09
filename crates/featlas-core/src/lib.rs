//! featlas canonical semantic core.
//!
//! This crate is the single authoritative implementation of the feature
//! engineering contract: restricted parsing, local schema validation, the
//! typed model, semantic/type checks, canonical hashing, target-neutral
//! planning, exact local evaluation and the target-neutral consumer API.
//! It contains no vendor SDK imports, no physical-coordinate assumptions and
//! no dispatch on vendor names: adapters are resolved through a registry keyed
//! by the namespaced implementation string declared in a deployment.

pub mod diag;
pub mod parse;
pub mod schema;
pub mod time;
pub mod expr;
pub mod load;
pub mod model;
pub mod validate;
pub mod value;
pub mod canon;
pub mod consumer;
pub mod eval;
pub mod fixtures;
pub mod capability;
pub mod plan;
pub mod label;
pub mod migration;
