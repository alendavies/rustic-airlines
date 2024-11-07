#[derive(Debug, Clone, PartialEq, Default)]
pub struct EndpointState {
    pub heartbeat_state: HeartbeatState,
    pub application_state: ApplicationState,
}

impl EndpointState {
    pub fn new(application_state: ApplicationState, heartbeat_state: HeartbeatState) -> Self {
        Self {
            application_state,
            heartbeat_state,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct HeartbeatState {
    pub generation: u128,
    pub version: u32,
}

impl HeartbeatState {
    pub fn new(generation: u128, version: u32) -> Self {
        Self {
            generation,
            version,
        }
    }

    pub fn inc_version(&mut self) {
        self.version += 1;
    }
}

#[derive(Clone, PartialEq, Debug, Default)]
pub struct ApplicationState {
    pub status: NodeStatus,
    pub version: u32,
}

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub enum NodeStatus {
    #[default]
    Bootstrap = 0x0,
    Normal = 0x1,
    Leaving = 0x2,
    Removing = 0x3,
    Dead = 0x4,
}
