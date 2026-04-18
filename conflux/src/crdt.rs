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
        // Empty slice means client has no state - use default (empty) state vector
        let remote = if remote_sv.is_empty() {
            StateVector::default()
        } else {
            match StateVector::decode_v1(remote_sv) {
                Ok(sv) => sv,
                Err(_) => return vec![],
            }
        };
        txn.encode_state_as_update_v1(&remote)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::{GetString, Text, Transact};

    #[test]
    fn test_encode_diff_with_empty_state_vector() {
        let engine = CrdtEngine::new();

        // Apply some content to the document
        {
            let doc = engine.doc();
            let doc_guard = doc.read().unwrap();
            let text = doc_guard.get_or_insert_text("content");
            let mut txn = doc_guard.transact_mut();
            text.insert(&mut txn, 0, "hello world");
        }

        // Request diff with empty state vector (simulates fresh client)
        let diff = engine.encode_diff(&[]);

        // Diff should not be empty - it should contain the full document
        assert!(
            !diff.is_empty(),
            "empty state vector should return full document state"
        );

        // Verify the diff can be applied to a new document
        let new_engine = CrdtEngine::new();
        new_engine.apply_update(&diff);

        // Verify content was synced
        let new_doc = new_engine.doc();
        let doc_guard = new_doc.read().unwrap();
        let text = doc_guard.get_or_insert_text("content");
        let txn = doc_guard.transact();
        assert_eq!(text.get_string(&txn), "hello world");
    }

    #[test]
    fn test_encode_diff_with_valid_state_vector() {
        let engine = CrdtEngine::new();

        // Apply some content
        {
            let doc = engine.doc();
            let doc_guard = doc.read().unwrap();
            let text = doc_guard.get_or_insert_text("content");
            let mut txn = doc_guard.transact_mut();
            text.insert(&mut txn, 0, "initial");
        }

        // Get state vector after initial content
        let sv = engine.state_vector();

        // Apply more content
        {
            let doc = engine.doc();
            let doc_guard = doc.read().unwrap();
            let text = doc_guard.get_or_insert_text("content");
            let mut txn = doc_guard.transact_mut();
            text.insert(&mut txn, 7, " update");
        }

        // Request diff from old state vector - should only get the new changes
        let diff = engine.encode_diff(&sv);
        assert!(!diff.is_empty());

        // The diff should be smaller than full state (only contains " update")
        let full_state = engine.encode_diff(&[]);
        assert!(
            diff.len() < full_state.len(),
            "incremental diff should be smaller than full state"
        );

        // Apply full state first, then verify incremental works
        let new_engine = CrdtEngine::new();

        // First sync: get initial state
        let initial_diff = engine.encode_diff(&[]);
        new_engine.apply_update(&initial_diff);

        // Verify we got the full content
        let new_doc = new_engine.doc();
        let doc_guard = new_doc.read().unwrap();
        let text = doc_guard.get_or_insert_text("content");
        let txn = doc_guard.transact();
        assert_eq!(text.get_string(&txn), "initial update");
    }

    #[test]
    fn test_encode_diff_with_invalid_state_vector() {
        let engine = CrdtEngine::new();

        // Apply some content
        {
            let doc = engine.doc();
            let doc_guard = doc.read().unwrap();
            let text = doc_guard.get_or_insert_text("content");
            let mut txn = doc_guard.transact_mut();
            text.insert(&mut txn, 0, "hello");
        }

        // Request diff with invalid/garbage state vector
        let diff = engine.encode_diff(&[0xFF, 0xFF, 0xFF]);

        // Should return empty vec for invalid input
        assert!(
            diff.is_empty(),
            "invalid state vector should return empty diff"
        );
    }
}
