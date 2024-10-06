/// Functions used to build the different frames used by our system.
/// For now, they are all in this same crate.
/// But the most complex ones may require their own.

use crate::frame::enums::error_codes::ErrorCode;
use crate::frame::enums::opcode::Opcode;
use crate::frame::enums::version::Version;
use crate::frame::frame::Frame;
use crate::frame::header::FrameHeader;

//-------------------   REQUESTS   -------------------


pub fn create_startup_frame() -> Frame {
    let body: String = "CQL_VERSION=3.0.0".to_string();
    let body_length = body.len() as u32;

    Frame::new(
        FrameHeader::new(Version::RequestV3, 0, 0, Opcode::AuthResponse, body_length),
        body,
    )
}
pub fn create_auth_response_frame(auth_token: String) -> Frame {

    let body_length = auth_token.len() as u32;

    Frame::new(
        FrameHeader::new(Version::RequestV3, 0, 0, Opcode::AuthResponse, body_length),
        auth_token
    )
}


//-------------------   RESPONSES   -------------------
pub fn create_error_frame(error_code: ErrorCode, error_message: String) -> Frame {
    let body = format!("{}: {}", error_code as u32, error_message);
    let body_length = body.len() as u32;

    Frame::new(
        FrameHeader::new(Version::ResponseV3, 0, 0, Opcode::Error, body_length),
        body,
    )
}