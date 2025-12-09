use std::sync::{Arc, RwLock};
use yrs::{
    Doc, ReadTxn, StateVector, Transact, Update,
    updates::{decoder::Decode, encoder::Encode},
};

#[derive(Clone)]
pub struct CrdtEngine {
    doc: Arc<RwLock<Doc>>,
}

impl Default for CrdtEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl CrdtEngine {
    pub fn new() -> Self {
        Self {
            doc: Arc::new(RwLock::new(Doc::new())),
        }
    }

    pub fn doc(&self) -> Arc<RwLock<Doc>> {
        self.doc.clone()
    }

    pub fn apply_update(&self, update: &[u8]) {
        let doc = self.doc.read().unwrap();
        let mut txn = doc.transact_mut();
        if let Ok(update) = Update::decode_v1(update) {
            let _ = txn.apply_update(update);
        }
    }

    pub fn state_vector(&self) -> Vec<u8> {
        let doc = self.doc.read().unwrap();
        let txn = doc.transact();
        txn.state_vector().encode_v1()
    }

    pub fn encode_state_as_update(&self) -> Vec<u8> {
        let doc = self.doc.read().unwrap();
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&txn.state_vector())
    }

    pub fn encode_diff(&self, remote_sv: &[u8]) -> Vec<u8> {
        let doc = self.doc.read().unwrap();
        let txn = doc.transact();
        if let Ok(remote) = StateVector::decode_v1(remote_sv) {
            txn.encode_state_as_update_v1(&remote)
        } else {
            vec![]
        }
    }
}
