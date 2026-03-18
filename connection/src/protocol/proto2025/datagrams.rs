use crate::protocol::deku_helpers::{DekuPackedSize, PacketPacking};
use deku::{DekuContainerRead, DekuError, DekuRead, DekuWrite};

// ======== Data ========

#[derive(Clone, Debug, PartialEq, DekuRead, DekuWrite)]
#[deku(endian = "little", bit_order = "lsb")]
pub struct EchoDatagram {
    pub data: [u8; 100],
}
impl DekuPackedSize<100> for EchoDatagram {}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(endian = "little", bit_order = "lsb")]
pub struct BoardIdsResponseDatagram {
    pub main: [u32; 3],
    pub kicker: [u32; 3],
    pub dribbler: [u32; 3],
    pub motor_fl: [u32; 3],
    pub motor_fr: [u32; 3],
    pub motor_bl: [u32; 3],
    pub motor_br: [u32; 3],
}
impl DekuPackedSize<84> for BoardIdsResponseDatagram {}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(endian = "little", bit_order = "lsb")]
pub struct WriteKickCalibrationCommandDatagram {
    /// rfc3339-formatted local datetime, rounded to seconds
    /// (Example: 1979-05-27T07:32:00)
    pub datetime_str: [u8; 19],
    pub kicker_id: [u32; 3],
    pub linear: [f32; 3],
    pub chip: [f32; 3],
}
impl DekuPackedSize<55> for WriteKickCalibrationCommandDatagram {}

// ======== Structure - Command ========

#[derive(Clone, Copy, Debug, PartialEq, DekuRead, DekuWrite)]
#[deku(
    bits = 7,
    id_type = "u8",
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub enum CommandDatagramType {
    #[deku(id = 0)]
    Echo = 0,
    #[deku(id = 1)]
    ReadRobotId = 1,
    #[deku(id = 2)]
    ReadBoardIds = 2,
    #[deku(id = 3)]
    ToggleKickCalibration = 3,
    #[deku(id = 4)]
    WriteKickCalibration = 4,
}

impl CommandDatagramType {
    pub fn get_datagram_size(&self) -> usize {
        match self {
            CommandDatagramType::Echo => EchoDatagram::packed_size(),
            CommandDatagramType::ReadRobotId => 0,
            CommandDatagramType::ReadBoardIds => 0,
            CommandDatagramType::ToggleKickCalibration => 1,
            CommandDatagramType::WriteKickCalibration => {
                WriteKickCalibrationCommandDatagram::packed_size()
            }
        }
    }
}

/// Helper type for handling full command datagrams
#[derive(Clone, Debug)]
pub enum CommandDatagram {
    Echo(EchoDatagram),
    ReadRobotId,
    ReadBoardIds,
    ToggleKickCalibration(bool),
    WriteKickCalibration(WriteKickCalibrationCommandDatagram),
}

impl From<CommandDatagram> for (CommandDatagramType, Vec<u8>) {
    fn from(d: CommandDatagram) -> Self {
        match d {
            CommandDatagram::Echo(d) => (CommandDatagramType::Echo, d.pack_to_vec()),
            CommandDatagram::ReadRobotId => (CommandDatagramType::ReadRobotId, vec![]),
            CommandDatagram::ReadBoardIds => (CommandDatagramType::ReadBoardIds, vec![]),
            CommandDatagram::ToggleKickCalibration(d) => {
                (CommandDatagramType::ToggleKickCalibration, vec![d as u8])
            }
            CommandDatagram::WriteKickCalibration(d) => {
                (CommandDatagramType::WriteKickCalibration, d.pack_to_vec())
            }
        }
    }
}

// ======== Structure - Response ========

#[derive(Clone, Copy, Debug, PartialEq, DekuRead, DekuWrite)]
#[deku(
    bits = 7,
    id_type = "u8",
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub enum ResponseDatagramType {
    #[deku(id = 0)]
    Echo = 0,
    #[deku(id = 1)]
    RobotId = 1,
    #[deku(id = 2)]
    BoardIds = 2,
}

impl ResponseDatagramType {
    pub fn get_datagram_size(&self) -> usize {
        match self {
            ResponseDatagramType::Echo => EchoDatagram::packed_size(),
            ResponseDatagramType::RobotId => size_of::<u8>(),
            ResponseDatagramType::BoardIds => BoardIdsResponseDatagram::packed_size(),
        }
    }
}

/// Helper type for handling full response datagrams
#[derive(Clone, Debug)]
pub enum ResponseDatagram {
    Echo(EchoDatagram),
    RobotId(u8),
    BoardIds(BoardIdsResponseDatagram),
}

impl TryFrom<(ResponseDatagramType, &[u8])> for ResponseDatagram {
    type Error = DekuError;
    fn try_from((datagram_type, data): (ResponseDatagramType, &[u8])) -> Result<Self, Self::Error> {
        Ok(match datagram_type {
            ResponseDatagramType::Echo => {
                ResponseDatagram::Echo(EchoDatagram::from_bytes((data, 0))?.1)
            }
            ResponseDatagramType::RobotId => ResponseDatagram::RobotId(data[0]),
            ResponseDatagramType::BoardIds => {
                ResponseDatagram::BoardIds(BoardIdsResponseDatagram::from_bytes((data, 0))?.1)
            }
        })
    }
}
