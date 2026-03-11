use datagrams::*;
use deku::prelude::*;
use deku_helpers::*;
use std::f32::consts::PI;

pub mod datagrams;
pub(crate) mod deku_helpers;

// All the ctx attributes are needed because endianness and bitsize are passed as context,
// which any nested structs have to explicitly accept

pub const PACKET_SIZE: usize = 29;
pub const DATAGRAM_CHUNK_SIZE: usize = PACKET_SIZE - 2;
pub const MAX_CHUNKS_PER_DATAGRAM: usize = 20;

/*
 * ============
 * = Mappings =
 * ============
 */

// Command
const DRIBBLER_CODEC: FloatCodec = FloatCodec::new(8, -1.0, 1.0);
const POS_CODEC: FloatCodec = FloatCodec::new(11, -8.0, 8.0);
const VEL_CODEC: FloatCodec = FloatCodec::new(12, -6.0, 6.0);
const ACC_CODEC: FloatCodec = FloatCodec::new(13, -15.0, 15.0);
const JERK_CODEC: FloatCodec = FloatCodec::new(15, -100.0, 100.0);
const ANGLE_CODEC: FloatCodec = FloatCodec::new(9, -PI, PI);
const ANGLE_VEL_CODEC: FloatCodec = FloatCodec::new(10, -20.0 * PI, 20.0 * PI);
const ANGLE_ACC_CODEC: FloatCodec = FloatCodec::new(14, -15000.0, 15000.0);
const ANGLE_JERK_CODEC: FloatCodec = FloatCodec::new(17, -400000.0, 400000.0);
const TRAJECTORY_PATH_ALPHA_CODEC: FloatCodec = FloatCodec::new(15, -PI, PI);
const TRAJECTORY_PATH_T_CODEC: FloatCodec = FloatCodec::new(15, 0.0, 40.0);
const TRAJECTORY_PATH_ACC_CODEC: FloatCodec = FloatCodec::new(12, 0.0, 15.0);
const TRAJECTORY_PATH_VEL_CODEC: FloatCodec = FloatCodec::new(12, 0.0, 6.0);
const TRAJECTORY_PATH_SLOW_DOWN_TIME_CODEC: FloatCodec = FloatCodec::new(9, 0.0, 1.0);
// Response
const LOAD_TORQUE_CODEC: FloatCodec = FloatCodec::new(8, -10.0, 10.0);
const BATTERY_CODEC: FloatCodec = FloatCodec::new(8, 0.0, 1.0);
const PACKET_LOSS_CODEC: FloatCodec = FloatCodec::new(8, 0.0, 1.0);
const MEASURED_POS_CODEC: FloatCodec = POS_CODEC.with_bits(14);
const MEASURED_VEL_CODEC: FloatCodec = VEL_CODEC.with_bits(15);
const MEASURED_ANGLE_CODEC: FloatCodec = ANGLE_CODEC.with_bits(14);
const MEASURED_ANGLE_VEL_CODEC: FloatCodec = ANGLE_VEL_CODEC.with_bits(14);

fn normalize_angle(mut angle: f32) -> f32 {
    while angle < -PI {
        angle += 2.0 * PI;
    }
    while angle >= PI {
        angle -= 2.0 * PI;
    }
    angle
}

/*
 * ===========
 * = Packets =
 * ===========
 */

#[derive(Clone, Debug, PartialEq, DekuRead, DekuWrite)]
#[deku(endian = "little", bit_order = "lsb")]
pub struct RadioPacket<RegularPayload, DatagramType>
where
    RegularPayload: DekuReader<'static, (deku::ctx::Endian, deku::ctx::Order)>
        + DekuWriter<(deku::ctx::Endian, deku::ctx::Order)>,
    DatagramType: DekuReader<'static, (deku::ctx::Endian, deku::ctx::Order)>
        + DekuWriter<(deku::ctx::Endian, deku::ctx::Order)>,
{
    /// Overflowing packet counter to determine packet loss
    #[deku(bits = 6)]
    pub counter: u8,
    /// The [`seqnum`](RadioPacketPayload::Datagram::seqnum) of the last received datagram packet
    #[deku(bits = 1)]
    pub acknum: u8,
    // 1Bit tag included here
    pub payload: RadioPacketPayload<RegularPayload, DatagramType>,
}

#[derive(Clone, Debug, PartialEq, DekuRead, DekuWrite)]
#[deku(
    bits = 1,
    id_type = "u8",
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub enum RadioPacketPayload<RegularPayload, DatagramType>
where
    RegularPayload: DekuReader<'static, (deku::ctx::Endian, deku::ctx::Order)>
        + DekuWriter<(deku::ctx::Endian, deku::ctx::Order)>,
    DatagramType: DekuReader<'static, (deku::ctx::Endian, deku::ctx::Order)>
        + DekuWriter<(deku::ctx::Endian, deku::ctx::Order)>,
{
    #[deku(id = 0)]
    Regular(RegularPayload),
    #[deku(id = 1)]
    Datagram {
        /// Alternating sequence number. Used to deduplicate packets on the receiving side (Can happen when the ack is lost)
        #[deku(bits = 1)]
        seqnum: u8,
        datagram_type: DatagramType, // 7Bit tag
        data: [u8; DATAGRAM_CHUNK_SIZE],
    },
}

// ======== Command (PC -> Robot) - Start ========

pub type RadioCommandPacket = RadioPacket<RegularCommandData, CommandDatagramType>;
impl FixedSizePacking<PACKET_SIZE> for RadioCommandPacket {}
pub type RadioCommandPayload = RadioPacketPayload<RegularCommandData, CommandDatagramType>;

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct RegularCommandData {
    /// Radio system processing delay
    pub time_offset: i8,

    /// `-1.0` to `1.0`
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, DRIBBLER_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, DRIBBLER_CODEC, self.dribbler)"
    )]
    pub dribbler: f32,
    /// `0`: Disable kicker
    ///
    /// `1-255`: Enable kicker with set power. Conversion to speed/chip distance:
    /// ```rust,ignore
    /// static MAX_SHOT_SPEED: u32 = 10;
    /// static MAX_CHIP_DIST: u32 = 5;
    /// if chip {
    ///     shot_power / 255 * MAX_CHIP_DIST
    /// } else {
    ///     shot_power / 255 * MAX_SHOT_SPEED
    /// }
    /// ```
    // TODO: Automatic shot power conversion
    pub shot_power: u8,
    /// `false`: Flat kick, `true`: Chip
    #[deku(bits = 1)]
    pub chip: bool,
    /// `false`: Discharge kick/chip capacitors, `true`: Charge kick/chip capacitors
    #[deku(bits = 1)]
    pub charge: bool,
    /// `false`: Kick on break beam detection, `true`: Force kick as soon as the kick capacitors are charged
    #[deku(bits = 1)]
    pub force_kick: bool,

    #[deku(bits = 1)]
    pub standby: bool,
    /// `true`: eject SD card and turn off
    #[deku(bits = 1)]
    pub eject_sdcard: bool,

    #[deku(bits = 1)]
    pub has_detection: bool,
    /// Current x position, as detected by the vision
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, POS_CODEC, self.detection_pos_x)"
    )]
    pub detection_pos_x: f32,
    /// Current y position, as detected by the vision
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, POS_CODEC, self.detection_pos_y)"
    )]
    pub detection_pos_y: f32,
    /// Current angular velocity in mrad/s, as detected by the vision
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ANGLE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ANGLE_CODEC, normalize_angle(self.detection_phi))"
    )]
    pub detection_phi: f32,

    #[deku(bits = 7)]
    pub unused: u8,

    // 4-bit tag included here
    pub trajectory: Trajectory,
}

/// Current target trajectory. For explanations and usage examples
#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    bits = 4,
    id_type = "u8",
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub enum Trajectory {
    #[default]
    #[deku(id = 0)]
    Halt,
    #[deku(id = 1)]
    Path(TrajectoryPath),
    #[deku(id = 2)]
    GlobalSpline(Spline),
    #[deku(id = 3)]
    LocalSpline(Spline),
}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct TrajectoryPath {
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, POS_CODEC, self.start_pos_x)"
    )]
    pub start_pos_x: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, POS_CODEC, self.start_pos_y)"
    )]
    pub start_pos_y: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ANGLE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ANGLE_CODEC, normalize_angle(self.start_phi))"
    )]
    pub start_phi: f32,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, VEL_CODEC, self.start_vel_x)"
    )]
    pub start_vel_x: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, VEL_CODEC, self.start_vel_y)"
    )]
    pub start_vel_y: f32,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ANGLE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ANGLE_CODEC, normalize_angle(self.end_phi))"
    )]
    pub end_phi: f32,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, VEL_CODEC, self.end_vel_x)"
    )]
    pub end_vel_x: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, VEL_CODEC, self.end_vel_x)"
    )]
    pub end_vel_y: f32,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, TRAJECTORY_PATH_ALPHA_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, TRAJECTORY_PATH_ALPHA_CODEC, normalize_angle(self.alpha))"
    )]
    pub alpha: f32,
    #[deku(
        reader = "float_from_uint(deku::reader, endian, bit_order, TRAJECTORY_PATH_T_CODEC)",
        writer = "float_to_uint(deku::writer, endian, bit_order, TRAJECTORY_PATH_T_CODEC, self.t)"
    )]
    pub t: f32,

    #[deku(
        reader = "float_from_uint(deku::reader, endian, bit_order, TRAJECTORY_PATH_ACC_CODEC)",
        writer = "float_to_uint(deku::writer, endian, bit_order, TRAJECTORY_PATH_ACC_CODEC, self.acceleration)"
    )]
    pub acceleration: f32,
    #[deku(
        reader = "float_from_uint(deku::reader, endian, bit_order, TRAJECTORY_PATH_VEL_CODEC)",
        writer = "float_to_uint(deku::writer, endian, bit_order, TRAJECTORY_PATH_VEL_CODEC, self.v_max)"
    )]
    pub v_max: f32,

    #[deku(
        reader = "float_from_uint(deku::reader, endian, bit_order, TRAJECTORY_PATH_SLOW_DOWN_TIME_CODEC)",
        writer = "float_to_uint(deku::writer, endian, bit_order, TRAJECTORY_PATH_SLOW_DOWN_TIME_CODEC, self.slow_down_time)"
    )]
    pub slow_down_time: f32,
    #[deku(bits = 1)]
    pub is_fast_endspeed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct Spline {
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, POS_CODEC, self.x_pos)"
    )]
    pub x_pos: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, VEL_CODEC, self.x_vel)"
    )]
    pub x_vel: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ACC_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ACC_CODEC, self.x_acc)"
    )]
    pub x_acc: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, JERK_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, JERK_CODEC, self.x_jerk)"
    )]
    pub x_jerk: f32,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, POS_CODEC, self.y_pos)"
    )]
    pub y_pos: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, VEL_CODEC, self.y_vel)"
    )]
    pub y_vel: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ACC_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ACC_CODEC, self.y_acc)"
    )]
    pub y_acc: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, JERK_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, JERK_CODEC, self.y_jerk)"
    )]
    pub y_jerk: f32,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ANGLE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ANGLE_CODEC, normalize_angle(self.phi_pos))"
    )]
    pub phi_pos: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ANGLE_VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ANGLE_VEL_CODEC, self.phi_vel)"
    )]
    pub phi_vel: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ANGLE_ACC_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ANGLE_ACC_CODEC, self.phi_acc)"
    )]
    pub phi_acc: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, ANGLE_JERK_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, ANGLE_JERK_CODEC, self.phi_jerk)"
    )]
    pub phi_jerk: f32,
}

// ======== Command (PC -> Robot) - End ========

// ======== Response (Robot -> PC) - Start ========

pub type RadioResponsePacket = RadioPacket<RegularResponseData, ResponseDatagramType>;
impl FixedSizePacking<PACKET_SIZE> for RadioResponsePacket {}
pub type RadioResponsePayload = RadioPacketPayload<RegularResponseData, ResponseDatagramType>;

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct MotorStatus {
    #[deku(bits = 1)]
    pub error: bool,
    #[deku(bits = 1)]
    pub overheated: bool,
    #[deku(bits = 1)]
    pub encoder_error: bool,
    #[deku(bits = 5)]
    pub unused: u8,
}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct KickerStatus {
    #[deku(bits = 1)]
    pub error: bool,
    #[deku(bits = 1)]
    pub break_beam_error: bool,
    #[deku(bits = 6)]
    pub unused: u8,
}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct IMUStatus {
    #[deku(bits = 1)]
    pub error: bool,
    #[deku(bits = 7)]
    pub unused: u8,
}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct SdStatus {
    #[deku(bits = 1)]
    pub error: bool,
    #[deku(bits = 1)]
    pub mounted: bool,
    #[deku(bits = 1)]
    pub full: bool,
    #[deku(bits = 5)]
    pub unused: u8,
}

#[derive(Clone, Debug, Default, PartialEq, DekuRead, DekuWrite)]
#[deku(
    endian = "endian",
    bit_order = "bit_order",
    ctx = "endian: deku::ctx::Endian, bit_order: deku::ctx::Order"
)]
pub struct RegularResponseData {
    #[deku(
        reader = "float_from_uint(deku::reader, endian, bit_order, BATTERY_CODEC)",
        writer = "float_to_uint(deku::writer, endian, bit_order, BATTERY_CODEC, self.battery)"
    )]
    pub battery: f32,
    #[deku(
        reader = "float_from_uint(deku::reader, endian, bit_order, PACKET_LOSS_CODEC)",
        writer = "float_to_uint(deku::writer, endian, bit_order, PACKET_LOSS_CODEC, self.packet_loss)"
    )]
    pub packet_loss: f32,

    // 1 byte each
    pub motor0_status: MotorStatus,
    pub motor1_status: MotorStatus,
    pub motor2_status: MotorStatus,
    pub motor3_status: MotorStatus,
    pub dribbler_status: MotorStatus,
    pub kicker_status: KickerStatus,
    pub imu_status: IMUStatus,
    pub sd_status: SdStatus,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, LOAD_TORQUE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, LOAD_TORQUE_CODEC, self.motor0_load_torque)"
    )]
    pub motor0_load_torque: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, LOAD_TORQUE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, LOAD_TORQUE_CODEC, self.motor1_load_torque)"
    )]
    pub motor1_load_torque: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, LOAD_TORQUE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, LOAD_TORQUE_CODEC, self.motor2_load_torque)"
    )]
    pub motor2_load_torque: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, LOAD_TORQUE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, LOAD_TORQUE_CODEC, self.motor3_load_torque)"
    )]
    pub motor3_load_torque: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, LOAD_TORQUE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, LOAD_TORQUE_CODEC, self.dribbler_load_torque)"
    )]
    pub dribbler_load_torque: f32,

    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, MEASURED_POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, MEASURED_POS_CODEC, self.measured_pos_x)"
    )]
    pub measured_pos_x: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, MEASURED_POS_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, MEASURED_POS_CODEC, self.measured_pos_y)"
    )]
    pub measured_pos_y: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, MEASURED_ANGLE_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, MEASURED_ANGLE_CODEC, self.measured_phi)"
    )]
    pub measured_phi: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, MEASURED_VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, MEASURED_VEL_CODEC, self.measured_vel_x)"
    )]
    pub measured_vel_x: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, MEASURED_VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, MEASURED_VEL_CODEC, self.measured_vel_y)"
    )]
    pub measured_vel_y: f32,
    #[deku(
        reader = "float_from_int(deku::reader, endian, bit_order, MEASURED_ANGLE_VEL_CODEC)",
        writer = "float_to_int(deku::writer, endian, bit_order, MEASURED_ANGLE_VEL_CODEC, self.measured_omega)"
    )]
    pub measured_omega: f32,

    #[deku(bits = 1)]
    pub power_enabled: bool,
    #[deku(bits = 1)]
    pub ball_detected: bool,

    pub unused: u16,
}

// ======== Response (Robot -> PC) - End ========
