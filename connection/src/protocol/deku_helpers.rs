use bitvec::field::BitField;
use deku::error::NeedSize;
use deku::prelude::{Reader, Writer};
use deku::{DekuContainerRead, DekuContainerWrite, DekuError, DekuWriter};

/// Deku tightly packs enums, but the packets are fixed-size, regardless of which C-union is used.
/// This trait provides fixed-size packing functions that can make enums at the end of a struct
/// replicate the padding behavior of C unions.
pub trait PacketPacking<const SIZE: usize> {
    fn packed_size() -> usize {
        SIZE
    }

    fn pack_to_slice(&self, slice: &mut [u8]);
    fn pack_to_vec(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; SIZE];
        self.pack_to_slice(&mut bytes);
        bytes
    }
}

impl<const N: usize> PacketPacking<N> for [u8; N] {
    fn pack_to_slice(&self, slice: &mut [u8]) {
        slice[..N].copy_from_slice(self);
    }
}
impl<const N: usize> PacketPacking<N> for Box<[u8; N]> {
    fn pack_to_slice(&self, slice: &mut [u8]) {
        slice[..N].copy_from_slice(&**self);
    }
}

/// Reverse of [PacketPacking]
pub trait PacketUnpacking<const SIZE: usize> {
    fn packed_size() -> usize {
        SIZE
    }
    fn unpack(slice: &[u8]) -> Result<Self, DekuError>
    where
        Self: Sized;
}

impl<const N: usize> PacketUnpacking<N> for [u8; N] {
    fn unpack(slice: &[u8]) -> Result<Self, DekuError> {
        if slice.len() < N {
            return Err(DekuError::Incomplete(NeedSize::new(N * 8)));
        }
        let mut out = [0u8; N];
        out.copy_from_slice(&slice[..N]);
        Ok(out)
    }
}

impl<const N: usize> PacketUnpacking<N> for Box<[u8; N]> {
    fn unpack(slice: &[u8]) -> Result<Self, DekuError> {
        if slice.len() < N {
            return Err(DekuError::Incomplete(NeedSize::new(N * 8)));
        }
        let mut out = Box::new([0u8; N]);
        out.copy_from_slice(&slice[..N]);
        Ok(out)
    }
}

/// Marker trait providing PacketPacking and PacketUnpacking implementations for deku structs
pub trait DekuPackedSize<const SIZE: usize>:
    DekuContainerWrite + for<'a> DekuContainerRead<'a>
{
}
impl<T: DekuPackedSize<SIZE>, const SIZE: usize> PacketPacking<SIZE> for T {
    fn pack_to_slice(&self, slice: &mut [u8]) {
        _ = self.to_slice(slice).unwrap();
        let written = self.to_slice(slice).unwrap();
        #[allow(clippy::needless_range_loop)]
        for i in written..SIZE {
            slice[i] = 0;
        }
    }
}
impl<T: DekuPackedSize<SIZE>, const SIZE: usize> PacketUnpacking<SIZE> for T {
    fn unpack(slice: &[u8]) -> Result<Self, DekuError> {
        Self::from_bytes((slice, 0)).map(|x| x.1)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FloatCodec {
    pub bits: usize,
    pub min: f32,
    pub max: f32,
}

impl FloatCodec {
    pub const fn new(bits: usize, min: f32, max: f32) -> Self {
        assert!(min < max);
        assert!(bits > 0);
        Self { bits, min, max }
    }

    pub const fn with_bits(&self, bits: usize) -> Self {
        Self { bits, ..*self }
    }
}

pub fn float_from_int<R: std::io::Read + std::io::Seek>(
    reader: &mut Reader<R>,
    endian: deku::ctx::Endian,
    bit_order: deku::ctx::Order,
    codec: FloatCodec,
) -> Result<f32, DekuError> {
    let y = reader
        .read_bits(codec.bits, bit_order)?
        .map(|b| {
            // FIXME: Something is definitely wrong here, but swapping these was neccessary to pass the rust<>c parity tests.
            if endian == deku::ctx::Endian::Big {
                b.load_le::<i32>()
            } else {
                b.load_be::<i32>()
            }
        })
        .unwrap();

    let y_max = (1 << (codec.bits - 1)) - 1;
    let y_min = -(1 << (codec.bits - 1));

    let x = codec.min + ((codec.max - codec.min) * (y - y_min) as f32) / (y_max - y_min) as f32;
    Ok(x)
}

pub fn float_to_int<W: std::io::Write + std::io::Seek>(
    writer: &mut Writer<W>,
    endian: deku::ctx::Endian,
    bit_order: deku::ctx::Order,
    codec: FloatCodec,
    x: f32,
) -> Result<(), DekuError> {
    let x = x.clamp(codec.min, codec.max) as f64;

    let x_max = codec.max as f64;
    let x_min = codec.min as f64;
    let y_max = (1 << (codec.bits - 1)) - 1;
    let y_min = -(1 << (codec.bits - 1));
    let y_maxf = y_max as f64;
    let y_minf = y_min as f64;

    // To just slightly tip the rounding towards +inf, we add EPSILON.
    // That way, when mapping 0 from a symmetric interval [-a, a] to an N bit signed integer interval,
    // 0 is mapped to 0 instead of -1. Because otherwise, the call below would result in (-0.5).round(),
    // which equals -1.
    let y = (y_minf + ((y_maxf - y_minf) * (x - x_min)) / (x_max - x_min) + 1e-8).round() as i32;
    let y = y.clamp(y_min, y_max);

    y.to_writer(writer, (endian, deku::ctx::BitSize(codec.bits), bit_order))
}

pub fn float_from_uint<R: std::io::Read + std::io::Seek>(
    reader: &mut Reader<R>,
    endian: deku::ctx::Endian,
    bit_order: deku::ctx::Order,
    codec: FloatCodec,
) -> Result<f32, DekuError> {
    let y = reader
        .read_bits(codec.bits, bit_order)?
        .map(|b| {
            // FIXME: Something is definitely wrong here, but swapping these was neccessary to pass the rust<>c parity tests.
            if endian == deku::ctx::Endian::Big {
                b.load_le::<u32>()
            } else {
                b.load_be::<u32>()
            }
        })
        .unwrap();

    let y_max = (1 << codec.bits) - 1;

    let x = codec.min + ((codec.max - codec.min) * y as f32) / y_max as f32;
    Ok(x)
}

pub fn float_to_uint<W: std::io::Write + std::io::Seek>(
    writer: &mut Writer<W>,
    endian: deku::ctx::Endian,
    bit_order: deku::ctx::Order,
    codec: FloatCodec,
    x: f32,
) -> Result<(), DekuError> {
    let x = x.clamp(codec.min, codec.max) as f64;

    let x_max = codec.max as f64;
    let x_min = codec.min as f64;
    let y_max = (1u32 << codec.bits) - 1u32;
    let y_maxf = y_max as f64;

    let y = ((y_maxf * (x - x_min)) / (x_max - x_min)).round() as u32;
    let y = y.clamp(0, y_max);

    y.to_writer(writer, (endian, deku::ctx::BitSize(codec.bits), bit_order))
}
