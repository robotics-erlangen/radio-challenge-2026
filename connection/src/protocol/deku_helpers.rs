use bitvec::field::BitField;
use deku::error::NeedSize;
use deku::prelude::{Reader, Writer};
use deku::{DekuContainerRead, DekuContainerWrite, DekuError, DekuWriter};

/// Packed sizes cannot be inferred because deku calculates sizes for enums and vecs at runtime.
/// This trait provides safe fixed-size packing functions that can make enums at the end of a struct
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

/// Marker trait providing a PacketPacking and PacketUnpacking implementations for deku structs
pub trait DekuPackedSize<const SIZE: usize>:
    DekuContainerWrite + for<'a> DekuContainerRead<'a>
{
}
impl<T: DekuPackedSize<SIZE>, const SIZE: usize> PacketPacking<SIZE> for T {
    fn pack_to_slice(&self, slice: &mut [u8]) {
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
    bits: usize,
    min: f32,
    max: f32,
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
            if endian == deku::ctx::Endian::Big {
                b.load_be::<i32>()
            } else {
                b.load_le::<i32>()
            }
        })
        .unwrap();

    let y = y as f32;

    let y_max = ((1 << (codec.bits - 1)) - 1) as f32;
    let y_min = -(1 << (codec.bits - 1)) as f32;

    let x = codec.min + ((codec.max - codec.min) * (y - y_min)) / (y_max - y_min);
    Ok(x)
}

pub fn float_to_int<W: std::io::Write + std::io::Seek>(
    writer: &mut Writer<W>,
    endian: deku::ctx::Endian,
    bit_order: deku::ctx::Order,
    codec: FloatCodec,
    x: f32,
) -> Result<(), DekuError> {
    let x = x.clamp(codec.min, codec.max);

    let y_max = ((1 << (codec.bits - 1)) - 1) as f32;
    let y_min = -(1 << (codec.bits - 1)) as f32;

    // To just slightly tip the rounding towards +inf, we add EPSILON.
    // That way, when mapping 0 from a symmetric interval [-a, a] to an N bit signed integer interval,
    // 0 is mapped to 0 instead of -1. Because otherwise, the call below would result in (-0.5).round(),
    // which equals -1.
    let y = (y_min + ((y_max - y_min) * (x - codec.min)) / (codec.max - codec.min) + 10e-10).round()
        as i32;

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
            if endian == deku::ctx::Endian::Big {
                b.load_be::<u32>()
            } else {
                b.load_le::<u32>()
            }
        })
        .unwrap();

    let y = y as f32;

    let y_max = ((1 << codec.bits) - 1) as f32;
    let y_min = 0f32;

    let x = codec.min + ((codec.max - codec.min) * (y - y_min)) / (y_max - y_min);
    Ok(x)
}

pub fn float_to_uint<W: std::io::Write + std::io::Seek>(
    writer: &mut Writer<W>,
    endian: deku::ctx::Endian,
    bit_order: deku::ctx::Order,
    codec: FloatCodec,
    x: f32,
) -> Result<(), DekuError> {
    let x = x.clamp(codec.min, codec.max);

    let y_max = ((1 << codec.bits) - 1) as f32;
    let y_min = 0f32;

    let y = (y_min + ((y_max - y_min) * (x - codec.min)) / (codec.max - codec.min)).round() as u32;

    y.to_writer(writer, (endian, deku::ctx::BitSize(codec.bits), bit_order))
}
