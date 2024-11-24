#[derive(Debug, Clone, PartialEq, Default)]
/// Represents the state of the endpoint in the cluster at a given point in time.
///
/// ### Fields
/// - `heartbeat_state`: The heartbeat state of the endpoint.
/// - `application_state`: The application state of the endpoint.
pub struct EndpointState {
    pub heartbeat_state: HeartbeatState,
    pub application_state: ApplicationState,
}

impl EndpointState {
    /// Creates a new `EndpointState` with the given `application_state` and `heartbeat_state`.
    pub fn new(application_state: ApplicationState, heartbeat_state: HeartbeatState) -> Self {
        Self {
            application_state,
            heartbeat_state,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
/// Represents the heartbeat state of the endpoint in the cluster at a given point in time.
///
/// ### Fields
/// - `generation`: The generation of the node.
/// - `version`: The version of the node.
pub struct HeartbeatState {
    pub generation: u128,
    pub version: u32,
}

impl HeartbeatState {
    /// Creates a new `HeartbeatState` with the given `generation` and `version`.
    pub fn new(generation: u128, version: u32) -> Self {
        Self {
            generation,
            version,
        }
    }

    /// Increments the version of the `HeartbeatState`.
    pub fn inc_version(&mut self) {
        self.version += 1;
    }
}

#[derive(Clone, PartialEq, Debug, Default)]
/// Represents the application state of the endpoint in the cluster at a given point in time.
///
/// ### Fields
/// - `status`: The status of the node.
/// - `version`: The version of the ApplicationState.
pub struct ApplicationState {
    pub status: NodeStatus,
    pub version: u32,
}

#[derive(Clone, Copy, PartialEq, Debug, Default)]
/// Represents the status of the node in the cluster.
/// - `Bootstrap`: The node is bootstrapping.
/// - `Normal`: The node is in the cluster.
/// - `Leaving`: The node is leaving the cluster.
/// - `Removing`: The node is being removed from the cluster.
/// - `Dead`: The node is dead.
pub enum NodeStatus {
    #[default]
    /// The node is in the process of joining the cluster.
    Bootstrap = 0x0,
    /// The node is in the cluster, and is fully operational.
    Normal = 0x1,
    /// The node is in the process of leaving the cluster.
    Leaving = 0x2,
    /// The node is in the process of being removed from the cluster.
    Removing = 0x3,
    /// The node is dead. Rip.
    Dead = 0x4,
}

impl NodeStatus {
    pub fn is_dead(&self) -> bool {
        matches!(self, NodeStatus::Dead)
    }

    pub fn is_normal(&self) -> bool {
        matches!(self, NodeStatus::Normal)
    }

    pub fn is_leaving(&self) -> bool {
        matches!(self, NodeStatus::Leaving)
    }

    pub fn is_starting(&self) -> bool {
        matches!(self, NodeStatus::Bootstrap)
    }

    pub fn is_removing(&self) -> bool {
        matches!(self, NodeStatus::Removing)
    }

    pub fn is_alive(&self) -> bool {
        !self.is_dead()
    }
}
