use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]

pub enum Message {
    ClientHello(ClientHello),
    JoinDocument(JoinDocument),
    SyncRequest(SyncRequest),
    Update(Update),
    AwarenessUpdate(AwarenessUpdate),
    LeaveDocument(LeaveDocument),
    Ping(Ping),

    ServerHello(ServerHello),
    DocumentJoined(DocumentJoined),
    SyncResponse(SyncResponse),
    DocumentLeft(DocumentLeft),
    Error(ErrorMessage),
    Pong(Pong),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientHello {
    pub protocol_version: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_info: Option<UserInfo>,

}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserInfo {
    pub name: String,
    pub color: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerHello {
    pub session_id: String,
    pub server_version: String,
    pub supported_features: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_client_id: Option<String>
    
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinDocument {
    pub document_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,

    #[serde(default)]
    pub read_only: bool,

}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentJoined {
    pub document_id: String,
    pub participant_count: u32,
    pub permissions: Permissions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Permissions {
    pub can_write: bool,
    pub can_read: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaveDocument {
    pub document_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentLeft {
    pub document_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncRequest {
    pub document_id: String,
    
    #[serde(with= "base64_bytes")]
    pub state_vector: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncResponse {
    pub document_id: String,

    #[serde(with = "base64_bytes")]
    pub update: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub document_id: String,

    #[serde(with= "base64_bytes")]
    pub update: Vec<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,


}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AwarenessUpdate {
    pub document_id: String,

    #[serde(with= "base64_bytes")]
    pub awareness_update: Vec<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ping {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pong {

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorMessage {

    pub code: ErrorCode,
    pub message: String,
    pub recoverable: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<HashMap<String, String>>,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidMessage,
    DocumentNotFound,
    PermissionDenied,
    VersionMismatch,
    RateLimited,
    InternalError,
}

