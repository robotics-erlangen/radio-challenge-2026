use bitvec::field::BitField;
use deku::prelude::{Reader, Writer};
use deku::{DekuContainerWrite, DekuError, DekuWriter};

/// Packed sizes cannot be inferred because deku calculates sizes for enums and vecs at runtime.
/// This trait provides safe fixed-size packing methods that can make enums replicate the
/// behavior of C unions, as long as they only appear as the last element in a struct.
pub trait FixedSizePacking<const SIZE: usize>: DekuContainerWrite {
    fn packed_size() -> usize {
        SIZE
    }
    fn pack_padded(&self) -> Vec<u8> {
        let mut bytes = self.to_bytes().unwrap();
        bytes.resize(Self::packed_size(), 0);
        bytes
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
