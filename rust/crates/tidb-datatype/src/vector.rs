// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `pkg/types/vector.go` and `vector_functions.go` transcreation.

use std::cmp::Ordering;
use std::convert::Infallible;
use std::fmt;
use std::ops::{ControlFlow, Range};
use std::sync::Arc;
#[cfg(target_endian = "little")]
use std::sync::{Mutex, OnceLock, PoisonError};

#[cfg(target_endian = "little")]
use bytes::Buf;
use bytes::Bytes;
use smallvec::SmallVec;
#[cfg(target_endian = "little")]
use zerocopy::{FromBytes, IntoBytes};

/// Maximum dimension accepted by TiDB's VECTOR type.
pub const MAX_VECTOR_DIMENSION: usize = 16_383;

/// Vector construction, parsing, serialization, or arithmetic error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorError(String);

impl VectorError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for VectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for VectorError {}

/// A little-endian TiDB `VECTOR<FLOAT32>`.
///
/// Validated constructors reject nonfinite values. Wire reads retain
/// little-endian storage and preserve arbitrary `f32` bits. Borrowing a
/// native slice of an unaligned vector aligns the bytes on demand.
#[derive(Clone)]
pub struct VectorFloat32 {
    elements: VectorElements,
}

#[derive(Clone)]
enum VectorElements {
    Owned(Vec<f32>),
    Inline {
        values: [f32; INLINE_VECTOR_ELEMENTS],
        len: u8,
    },
    #[cfg(target_endian = "little")]
    Shared(Bytes),
    #[cfg(target_endian = "little")]
    Wire {
        data: Arc<SharedVectorBytes>,
        payload: Range<usize>,
    },
}

const INLINE_VECTOR_ELEMENTS: usize = 7;

/// An immutable packed vector column shared by its decoded vector values.
///
/// Native slice callers borrow aligned wire data or share an aligned copy.
/// Other operations can read the wire bytes directly.
pub struct SharedVectorBytes {
    bytes: Bytes,
    #[cfg(target_endian = "little")]
    aligned: OnceLock<AlignedVectorElements>,
    #[cfg(target_endian = "little")]
    aligned_spare: Mutex<AlignedVectorSpare>,
    #[cfg(target_endian = "little")]
    recent: [(usize, usize); RETAINED_VECTOR_FRAMES],
    #[cfg(target_endian = "little")]
    retained: Option<Box<AlignedVectorHistory>>,
}

#[cfg(target_endian = "little")]
struct AlignedVectorFrame {
    bytes: Bytes,
    aligned: OnceLock<AlignedVectorElements>,
}

#[cfg(target_endian = "little")]
#[derive(Default)]
struct AlignedVectorHistory {
    frames: [Option<AlignedVectorFrame>; RETAINED_VECTOR_FRAMES],
    next_slot: usize,
    misses: usize,
}

#[cfg(target_endian = "little")]
const RETAINED_VECTOR_FRAMES: usize = 3;

#[cfg(target_endian = "little")]
const MAX_RETAINED_VECTOR_FRAME_BYTES: usize = 2 * 1024 * 1024;

#[cfg(target_endian = "little")]
#[derive(Default)]
struct AlignedVectorSpare {
    first: Vec<f32>,
    whole: Vec<f32>,
}

#[cfg(target_endian = "little")]
enum AlignedVectorElements {
    Whole(Vec<f32>),
    First(Box<AlignedVectorCell>),
}

#[cfg(target_endian = "little")]
struct AlignedVectorCell {
    payload: Range<usize>,
    elements: Vec<f32>,
    whole: OnceLock<Vec<f32>>,
}

impl SharedVectorBytes {
    /// Retains the packed bytes without aligning or decoding them.
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            #[cfg(target_endian = "little")]
            aligned: OnceLock::new(),
            #[cfg(target_endian = "little")]
            aligned_spare: Mutex::new(AlignedVectorSpare::default()),
            #[cfg(target_endian = "little")]
            recent: [(0, 0); RETAINED_VECTOR_FRAMES],
            #[cfg(target_endian = "little")]
            retained: None,
        }
    }

    /// Replaces the immutable input after the last vector reader has released it.
    pub fn replace_bytes(&mut self, bytes: Bytes) {
        #[cfg(target_endian = "little")]
        {
            let next_range = (bytes.as_ptr().addr(), bytes.len());
            let old_range = (self.bytes.as_ptr().addr(), self.bytes.len());
            if (next_range.0 | old_range.0).is_multiple_of(4) && self.retained.is_none() {
                // Native views of these frames use the wire bytes directly.
                self.bytes = bytes;
                return;
            }
            if let Some(history) = &mut self.retained {
                if let Some(frame) = history.frames.iter_mut().flatten().find(|frame| {
                    (frame.bytes.as_ptr().addr(), frame.bytes.len()) == next_range
                }) {
                    std::mem::swap(&mut frame.bytes, &mut self.bytes);
                    std::mem::swap(&mut frame.aligned, &mut self.aligned);
                    history.misses = 0;
                    return;
                }
            }

            // An address remembered without its owner is only a hint to
            // start retaining. Hits always compare an owned byte range.
            let recurring = self.recent.contains(&next_range);
            self.recent.rotate_right(1);
            self.recent[0] = old_range;
            if self.aligned.get().is_some()
                && old_range.1 <= MAX_RETAINED_VECTOR_FRAME_BYTES
                && next_range.1 <= MAX_RETAINED_VECTOR_FRAME_BYTES
            {
                if recurring && self.retained.is_none() {
                    self.retained = Some(Box::default());
                }
                if let Some(history) = &mut self.retained {
                    // Allow enough replacements to fill the rotation. Drop
                    // inactive frames if inputs stop recurring.
                    if history.misses < RETAINED_VECTOR_FRAMES {
                        history.frames[history.next_slot] = Some(AlignedVectorFrame {
                            bytes: std::mem::replace(&mut self.bytes, bytes),
                            aligned: std::mem::take(&mut self.aligned),
                        });
                        history.next_slot = (history.next_slot + 1) % RETAINED_VECTOR_FRAMES;
                        history.misses += 1;
                        return;
                    }
                }
            }
            self.retained = None;

            let aligned = self.aligned.take();
            let spare = self
                .aligned_spare
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner);
            *spare = if !next_range.0.is_multiple_of(4) && bytes.len() == self.bytes.len() {
                match aligned {
                    Some(AlignedVectorElements::Whole(bytes)) => AlignedVectorSpare {
                        whole: bytes,
                        ..Default::default()
                    },
                    Some(AlignedVectorElements::First(cell)) => AlignedVectorSpare {
                        first: cell.elements,
                        whole: cell.whole.into_inner().unwrap_or_default(),
                    },
                    None => AlignedVectorSpare::default(),
                }
            } else {
                AlignedVectorSpare::default()
            };
        }
        self.bytes = bytes;
    }

    /// Borrows the original packed bytes.
    #[inline]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Reads the leading vector in a cell range contained in the column.
    ///
    /// Panics if the range is outside the supplied column bytes.
    #[inline]
    pub fn read(self: &Arc<Self>, cell: Range<usize>) -> Result<VectorFloat32, VectorError> {
        #[cfg(target_endian = "little")]
        if cell.start.is_multiple_of(4) {
            let len = peek_vector_float32(&self.bytes[cell.clone()])?;
            return Ok(VectorFloat32::from_wire(
                Arc::clone(self),
                cell.start + 4..cell.start + len,
            ));
        }
        VectorFloat32::from_shared_encoded(self.bytes.slice(cell))
    }

    #[cfg(target_endian = "little")]
    fn elements(&self, payload: Range<usize>) -> &[f32] {
        if payload.is_empty() {
            return &[];
        }
        let aligned = self.aligned.get_or_init(|| {
            // A single native read should not copy a much larger column.
            if payload.len() <= self.bytes.len() / 4 {
                AlignedVectorElements::First(Box::new(AlignedVectorCell {
                    payload: payload.clone(),
                    elements: self.align_elements(&self.bytes[payload.clone()], false),
                    whole: OnceLock::new(),
                }))
            } else {
                AlignedVectorElements::Whole(self.align_elements(&self.bytes, true))
            }
        });
        // Wire vector payloads handled by this owner start on a word boundary
        // relative to the column, regardless of its address alignment.
        let indices = payload.start / 4..payload.end / 4;
        match aligned {
            AlignedVectorElements::Whole(elements) => &elements[indices],
            AlignedVectorElements::First(cell) if cell.payload == payload => &cell.elements,
            AlignedVectorElements::First(cell) => {
                // Keep the first cell's address stable if another cell is read.
                &cell.whole.get_or_init(|| self.align_elements(&self.bytes, true))[indices]
            }
        }
    }

    #[cfg(target_endian = "little")]
    fn align_elements(&self, bytes: &[u8], whole: bool) -> Vec<f32> {
        let mut elements = {
            let mut spare = self
                .aligned_spare
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if whole {
                std::mem::take(&mut spare.whole)
            } else {
                std::mem::take(&mut spare.first)
            }
        };
        elements.clear();
        elements.extend(wire_elements(bytes));
        elements
    }
}

impl Default for VectorFloat32 {
    fn default() -> Self {
        Self::init(0)
    }
}

impl PartialEq for VectorFloat32 {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if let (VectorElements::Owned(left), VectorElements::Owned(right)) =
            (&self.elements, &other.elements)
        {
            return equal_vector_slices(left, right, |x| x, |x| x);
        }
        self.as_view().0.equal_elements(other.as_view().0)
    }
}

impl fmt::Debug for VectorFloat32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("VectorFloat32")
            .field(&self.elements())
            .finish()
    }
}

impl fmt::Display for VectorFloat32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values = self.as_view().0;
        if values.len() <= 4 {
            // Allow a sign, a decimal point, leading zeros and up to nine
            // significant digits. Send small vectors to the sink in one call.
            let mut bytes = [0; 4 * 57 + 2];
            let mut staging = VectorTextBuffer(&mut bytes);
            values.write_text(&mut staging)?;
            let remaining = staging.0.len();
            let len = bytes.len() - remaining;
            return formatter.write_str(
                std::str::from_utf8(&bytes[..len]).expect("formatted vector text is ASCII"),
            );
        }
        values.write_text(formatter)
    }
}

struct VectorTextBuffer<'a>(&'a mut [u8]);

impl fmt::Write for VectorTextBuffer<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let (head, tail) = std::mem::take(&mut self.0)
            .split_at_mut_checked(text.len())
            .ok_or(fmt::Error)?;
        head.copy_from_slice(text.as_bytes());
        self.0 = tail;
        Ok(())
    }
}

struct VectorTextBytes<'a>(&'a mut Vec<u8>);

impl fmt::Write for VectorTextBytes<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0.extend_from_slice(text.as_bytes());
        Ok(())
    }
}

impl VectorFloat32 {
    /// Creates a vector after rejecting NaN and infinity.
    pub fn create(elements: impl Into<Vec<f32>>) -> Result<Self, VectorError> {
        let elements = elements.into();
        for value in &elements {
            if value.is_nan() {
                return Err(VectorError::new("NaN not allowed in vector"));
            }
            if value.is_infinite() {
                return Err(VectorError::new("infinite value not allowed in vector"));
            }
        }
        Ok(Self::from_elements(elements))
    }

    /// Creates a vector and panics on an invalid value.
    pub fn must_create(elements: impl Into<Vec<f32>>) -> Self {
        Self::create(elements).unwrap_or_else(|error| panic!("{error}"))
    }

    /// Creates a zero-filled vector of the given dimension.
    pub fn init(dimensions: usize) -> Self {
        Self::from_elements(vec![0.0; dimensions])
    }

    fn from_elements(elements: Vec<f32>) -> Self {
        Self {
            elements: VectorElements::Owned(elements),
        }
    }

    #[cfg(target_endian = "little")]
    fn from_wire(data: Arc<SharedVectorBytes>, payload: Range<usize>) -> Self {
        Self {
            elements: VectorElements::Wire { data, payload },
        }
    }

    /// Reads the first vector from an immutable serialized buffer.
    /// Little-endian payloads remain shared, including unaligned payloads.
    pub fn from_shared_encoded(bytes: Bytes) -> Result<Self, VectorError> {
        let length = peek_vector_float32(&bytes)?;
        #[cfg(target_endian = "little")]
        {
            let mut bytes = bytes;
            bytes.truncate(length);
            bytes.advance(4);
            if <[f32]>::ref_from_bytes(&bytes).is_ok() {
                return Ok(Self {
                    elements: VectorElements::Shared(bytes),
                });
            }
            let len = bytes.len();
            Ok(Self::from_wire(
                Arc::new(SharedVectorBytes::new(bytes)),
                0..len,
            ))
        }
        #[cfg(target_endian = "big")]
        Ok(Self::from_elements(decode_vector_elements(
            &bytes[4..length],
        )))
    }

    /// Reads a vector from a subslice of the immutable backing vector.
    pub fn from_shared_subslice(owner: &Arc<Vec<u8>>, value: &[u8]) -> Result<Self, VectorError> {
        let value = tidb_goruntime::GoString::from_shared_subslice(owner, value);
        Self::from_shared_encoded(Bytes::from_owner(value))
    }

    /// Returns the vector dimension.
    pub const fn len(&self) -> usize {
        match &self.elements {
            VectorElements::Owned(elements) => elements.len(),
            VectorElements::Inline { len, .. } => *len as usize,
            #[cfg(target_endian = "little")]
            VectorElements::Shared(bytes) => bytes.len() / 4,
            #[cfg(target_endian = "little")]
            VectorElements::Wire { payload, .. } => (payload.end - payload.start) / 4,
        }
    }

    /// Returns whether this vector has no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrows the elements.
    #[inline]
    pub fn elements(&self) -> &[f32] {
        match &self.elements {
            VectorElements::Owned(elements) => elements,
            VectorElements::Inline { values, len } => &values[..*len as usize],
            #[cfg(target_endian = "little")]
            VectorElements::Shared(bytes) => {
                <[f32]>::ref_from_bytes(bytes).expect("shared vector payload remains aligned")
            }
            #[cfg(target_endian = "little")]
            VectorElements::Wire { data, payload }
                if data.bytes.as_ptr().addr().is_multiple_of(4) =>
            {
                <[f32]>::ref_from_bytes(&data.bytes[payload.clone()])
                    .expect("shared vector payload remains aligned")
            }
            #[cfg(target_endian = "little")]
            VectorElements::Wire { data, payload } => data.elements(payload.clone()),
        }
    }

    /// Mutably borrows the elements, matching Go `Elements`.
    ///
    /// Like the source, this does not revalidate values written by the caller.
    pub fn elements_mut(&mut self) -> &mut [f32] {
        #[cfg(target_endian = "little")]
        match &self.elements {
            VectorElements::Owned(_) | VectorElements::Inline { .. } => {}
            VectorElements::Shared(_) => {
                self.elements = VectorElements::Owned(self.elements().to_vec());
            }
            VectorElements::Wire { data, payload } => {
                self.elements =
                    VectorElements::Owned(decode_vector_elements(&data.bytes[payload.clone()]));
            }
        }
        match &mut self.elements {
            VectorElements::Owned(elements) => elements,
            VectorElements::Inline { values, len } => &mut values[..*len as usize],
            #[cfg(target_endian = "little")]
            VectorElements::Shared(_) | VectorElements::Wire { .. } => {
                unreachable!("shared elements were copied before mutation")
            }
        }
    }

    /// Checks the dimension declared by a column. `None` is unspecified.
    pub fn check_dims_fit_column(&self, expected: Option<usize>) -> Result<(), VectorError> {
        if let Some(expected) = expected {
            if self.len() != expected {
                return Err(VectorError::new(format!(
                    "vector has {} dimensions, does not fit VECTOR({expected})",
                    self.len()
                )));
            }
        }
        Ok(())
    }

    /// Returns the source log/EXPLAIN representation.
    pub fn truncated_string(&self) -> String {
        const DISPLAY: usize = 5;
        let dimensions = self.len();
        let mut output = String::with_capacity(2 + dimensions.min(DISPLAY) * 8);
        output.push('[');
        self.as_view().0.prefix(DISPLAY).fold((), |(), value| {
            if output.len() > 1 {
                output.push(',');
            }
            write_f32_general_two_digits(value, &mut output);
        });
        if dimensions > DISPLAY {
            use std::fmt::Write as _;
            write!(output, ",({} more)...", dimensions - DISPLAY).expect("writing to a string");
        }
        output.push(']');
        output
    }

    /// Appends the exact little-endian source wire representation.
    pub fn serialize_to(&self, destination: &mut Vec<u8>) {
        destination.extend_from_slice(&(self.len() as u32).to_le_bytes());
        #[cfg(target_endian = "little")]
        destination.extend_from_slice(match &self.elements {
            VectorElements::Owned(elements) => elements.as_bytes(),
            VectorElements::Inline { values, len } => values[..*len as usize].as_bytes(),
            VectorElements::Shared(bytes) => bytes,
            VectorElements::Wire { data, payload } => &data.bytes[payload.clone()],
        });
        #[cfg(target_endian = "big")]
        for value in self.elements() {
            destination.extend_from_slice(&value.to_bits().to_le_bytes());
        }
    }

    /// Returns the exact little-endian source wire representation.
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.serialized_size());
        self.serialize_to(&mut result);
        result
    }

    /// Returns the serialized byte length.
    pub const fn serialized_size(&self) -> usize {
        4 + self.len() * 4
    }

    /// Returns Go's estimated memory usage for the native pointer width.
    pub const fn estimated_mem_usage(&self) -> usize {
        // The source value consists of a Go slice pointer, length and capacity.
        3 * std::mem::size_of::<usize>() + self.serialized_size()
    }

    /// Parses the JSON-array text accepted by TiDB.
    pub fn parse(text: &str) -> Result<Self, VectorError> {
        parse_vector_text(text)
    }

    /// Returns whether this is the source zero value.
    pub fn is_zero_value(&self) -> bool {
        self.is_empty()
    }

    /// Lexicographically compares elements, then dimensions.
    pub fn compare(&self, other: &Self) -> Ordering {
        self.as_view().compare(&other.as_view())
    }

    fn check_identical_dims(&self, other: &Self) -> Result<(), VectorError> {
        check_identical_dims(self.len(), other.len())
    }

    /// Borrows the elements with their existing storage representation.
    #[inline]
    pub fn as_view(&self) -> VectorFloat32Ref<'_> {
        VectorFloat32Ref(match &self.elements {
            VectorElements::Owned(elements) => VectorSlice::Native(elements),
            VectorElements::Inline { values, len } => VectorSlice::Native(&values[..*len as usize]),
            #[cfg(target_endian = "little")]
            VectorElements::Shared(bytes) => VectorSlice::Native(
                <[f32]>::ref_from_bytes(bytes).expect("shared vector payload remains aligned"),
            ),
            #[cfg(target_endian = "little")]
            VectorElements::Wire { data, payload } => {
                VectorSlice::Wire(data.bytes[payload.clone()].as_chunks::<4>().0)
            }
        })
    }

    /// Squared L2 distance with source float32 accumulation.
    pub fn l2_squared_distance(&self, other: &Self) -> Result<f64, VectorError> {
        self.as_view().l2_squared_distance(&other.as_view())
    }

    /// Euclidean distance.
    pub fn l2_distance(&self, other: &Self) -> Result<f64, VectorError> {
        self.as_view().l2_distance(&other.as_view())
    }

    /// Inner product with source float32 accumulation.
    pub fn inner_product(&self, other: &Self) -> Result<f64, VectorError> {
        self.as_view().inner_product(&other.as_view())
    }

    /// Negative inner product.
    pub fn negative_inner_product(&self, other: &Self) -> Result<f64, VectorError> {
        self.as_view().negative_inner_product(&other.as_view())
    }

    /// Cosine distance with source clamping and zero-vector NaN behavior.
    pub fn cosine_distance(&self, other: &Self) -> Result<f64, VectorError> {
        self.as_view().cosine_distance(&other.as_view())
    }

    /// Manhattan distance with source float32 accumulation.
    pub fn l1_distance(&self, other: &Self) -> Result<f64, VectorError> {
        self.as_view().l1_distance(&other.as_view())
    }

    /// L2 norm with the source's intentional float64 accumulation.
    pub fn l2_norm(&self) -> f64 {
        self.as_view().l2_norm()
    }

    /// Element-wise addition.
    pub fn add(&self, other: &Self) -> Result<Self, VectorError> {
        self.elementwise(other, |left, right| left + right)
    }

    /// Element-wise subtraction.
    pub fn sub(&self, other: &Self) -> Result<Self, VectorError> {
        self.elementwise(other, |left, right| left - right)
    }

    /// Element-wise multiplication.
    pub fn mul(&self, other: &Self) -> Result<Self, VectorError> {
        self.elementwise(other, |left, right| left * right)
    }

    fn elementwise(
        &self,
        other: &Self,
        operation: impl Fn(f32, f32) -> f32,
    ) -> Result<Self, VectorError> {
        self.check_identical_dims(other)?;
        let elements = self.as_view().0.map_pair(other.as_view().0, operation);
        // Reduce the finite check in batches, then report the first error in
        // input order when any result is nonfinite.
        if elements
            .iter()
            .fold(false, |bad, value| bad | !value.is_finite())
        {
            let value = elements.iter().find(|value| !value.is_finite()).unwrap();
            return Err(VectorError::new(if value.is_infinite() {
                "value out of range: overflow"
            } else {
                "value out of range: NaN"
            }));
        }
        Ok(Self::from_elements(elements))
    }
}

/// A vector whose elements can borrow the serialized chunk cell for an operation.
pub struct VectorFloat32Ref<'a>(VectorSlice<'a>);

#[derive(Clone, Copy)]
enum VectorSlice<'a> {
    Native(&'a [f32]),
    Wire(&'a [[u8; 4]]),
}

impl<'a> VectorSlice<'a> {
    fn len(self) -> usize {
        match self {
            Self::Native(elements) => elements.len(),
            Self::Wire(elements) => elements.len(),
        }
    }

    fn prefix(self, limit: usize) -> Self {
        let end = limit.min(self.len());
        match self {
            Self::Native(elements) => Self::Native(&elements[..end]),
            Self::Wire(elements) => Self::Wire(&elements[..end]),
        }
    }

    fn write_text(self, output: &mut impl fmt::Write) -> fmt::Result {
        match self {
            Self::Native(elements) => write_vector_text(elements.iter().copied(), output),
            Self::Wire(elements) => {
                write_vector_text(elements.iter().copied().map(f32::from_le_bytes), output)
            }
        }
    }

    #[inline]
    fn equal_elements(self, other: Self) -> bool {
        match (self, other) {
            (Self::Native(a), Self::Native(b)) => equal_vector_slices(a, b, |x| x, |x| x),
            (Self::Native(a), Self::Wire(b)) => {
                equal_vector_slices(a, b, |x| x, f32::from_le_bytes)
            }
            (Self::Wire(a), Self::Native(b)) => {
                equal_vector_slices(a, b, f32::from_le_bytes, |x| x)
            }
            (Self::Wire(a), Self::Wire(b)) => {
                equal_vector_slices(a, b, f32::from_le_bytes, f32::from_le_bytes)
            }
        }
    }

    #[inline]
    fn map_pair(self, other: Self, operation: impl Fn(f32, f32) -> f32) -> Vec<f32> {
        let operation = |(left, right)| operation(left, right);
        match (self, other) {
            (Self::Native(a), Self::Native(b)) => a
                .iter()
                .copied()
                .zip(b.iter().copied())
                .map(operation)
                .collect(),
            (Self::Native(a), Self::Wire(b)) => a
                .iter()
                .copied()
                .zip(b.iter().copied().map(f32::from_le_bytes))
                .map(operation)
                .collect(),
            (Self::Wire(a), Self::Native(b)) => a
                .iter()
                .copied()
                .map(f32::from_le_bytes)
                .zip(b.iter().copied())
                .map(operation)
                .collect(),
            (Self::Wire(a), Self::Wire(b)) => a
                .iter()
                .copied()
                .map(f32::from_le_bytes)
                .zip(b.iter().copied().map(f32::from_le_bytes))
                .map(operation)
                .collect(),
        }
    }

    #[inline]
    fn fold<T>(self, start: T, update: impl FnMut(T, f32) -> T) -> T {
        match self {
            Self::Native(elements) => elements.iter().copied().fold(start, update),
            Self::Wire(elements) => elements
                .iter()
                .copied()
                .map(f32::from_le_bytes)
                .fold(start, update),
        }
    }

    #[inline]
    fn fold_pair<T>(self, other: Self, start: T, mut update: impl FnMut(T, f32, f32) -> T) -> T {
        match self.try_fold_pair(other, start, |state, left, right| {
            ControlFlow::<Infallible, _>::Continue(update(state, left, right))
        }) {
            ControlFlow::Continue(result) => result,
            ControlFlow::Break(never) => match never {},
        }
    }

    #[inline]
    fn try_fold_pair<B, T>(
        self,
        other: Self,
        start: T,
        mut update: impl FnMut(T, f32, f32) -> ControlFlow<B, T>,
    ) -> ControlFlow<B, T> {
        // Select the backing once per operation, before iterating through it.
        let reduce = |state, (left, right)| update(state, left, right);
        match (self, other) {
            (Self::Native(a), Self::Native(b)) => a
                .iter()
                .copied()
                .zip(b.iter().copied())
                .try_fold(start, reduce),
            (Self::Native(a), Self::Wire(b)) => a
                .iter()
                .copied()
                .zip(b.iter().copied().map(f32::from_le_bytes))
                .try_fold(start, reduce),
            (Self::Wire(a), Self::Native(b)) => a
                .iter()
                .copied()
                .map(f32::from_le_bytes)
                .zip(b.iter().copied())
                .try_fold(start, reduce),
            (Self::Wire(a), Self::Wire(b)) => a
                .iter()
                .copied()
                .map(f32::from_le_bytes)
                .zip(b.iter().copied().map(f32::from_le_bytes))
                .try_fold(start, reduce),
        }
    }
}

#[inline]
fn equal_vector_slices<T: Copy, U: Copy>(
    left: &[T],
    right: &[U],
    left_value: impl Fn(T) -> f32,
    right_value: impl Fn(U) -> f32,
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let Some(((first_left, left), (first_right, right))) =
        left.split_first().zip(right.split_first())
    else {
        return true;
    };
    let equal = |(a, b): (&T, &U)| left_value(*a) == right_value(*b);
    if !equal((first_left, first_right)) {
        return false;
    }
    let (left, left_tail) = left.as_chunks::<8>();
    let (right, right_tail) = right.as_chunks::<8>();
    // Check each block without branching per element so a long equal prefix
    // can use SIMD while a difference still stops the scan.
    left.iter()
        .zip(right)
        .all(|(a, b)| a.iter().zip(b).fold(true, |same, pair| same & equal(pair)))
        && left_tail.iter().zip(right_tail).all(equal)
}

#[inline]
fn first_wire_difference<'a>(
    pairs: impl Iterator<Item = (&'a [u8; 4], &'a [u8; 4])>,
) -> Option<Ordering> {
    for (&left, &right) in pairs {
        let left = f32::from_le_bytes(left);
        let right = f32::from_le_bytes(right);
        if left < right {
            return Some(Ordering::Less);
        }
        if left > right {
            return Some(Ordering::Greater);
        }
    }
    None
}

#[inline]
fn compare_wire_vectors(left: &[[u8; 4]], right: &[[u8; 4]]) -> Ordering {
    let ([first_left, left @ ..], [first_right, right @ ..]) = (left, right) else {
        return left.len().cmp(&right.len());
    };
    let first_left = f32::from_le_bytes(*first_left);
    let first_right = f32::from_le_bytes(*first_right);
    if first_left < first_right {
        return Ordering::Less;
    }
    if first_left > first_right {
        return Ordering::Greater;
    }
    if left.len().min(right.len()) < 16 {
        return first_wire_difference(left.iter().zip(right))
            .unwrap_or_else(|| left.len().cmp(&right.len()));
    }
    compare_wire_vector_blocks(left, right)
}

#[inline]
fn wire_vector_payload(bytes: &[u8]) -> Result<&[[u8; 4]], VectorError> {
    let Some((header, payload)) = bytes.as_chunks::<4>().0.split_first() else {
        return Err(vector_size_error(bytes.len(), None));
    };
    let dimensions = u32::from_le_bytes(*header);
    if dimensions >= u32::MAX / 4 {
        return Err(vector_size_overflow());
    }
    payload
        .get(..dimensions as usize)
        .ok_or_else(|| vector_size_error(bytes.len(), Some((dimensions as usize + 1) * 4)))
}

#[inline(never)]
fn compare_wire_vector_blocks(left: &[[u8; 4]], right: &[[u8; 4]]) -> Ordering {
    let dimensions = left.len().cmp(&right.len());
    let common = left.len().min(right.len());
    let left = &left[..common];
    let right = &right[..common];
    let (left_blocks, left_tail) = left.as_chunks::<16>();
    let (right_blocks, right_tail) = right.as_chunks::<16>();
    // Equal encoded blocks can skip numeric comparisons. Differing blocks
    // still use Go's float ordering, including ties for unordered values.
    for (left, right) in left_blocks.iter().zip(right_blocks) {
        if left != right {
            if let Some(order) = first_wire_difference(left.iter().zip(right)) {
                return order;
            }
        }
    }
    first_wire_difference(left_tail.iter().zip(right_tail)).unwrap_or(dimensions)
}

impl<'a> VectorFloat32Ref<'a> {
    /// Borrows the encoded vector without allocating. Like Go, accepts a
    /// valid leading vector with a suffix.
    #[inline]
    pub fn from_encoded(bytes: &'a [u8]) -> Result<Self, VectorError> {
        Ok(Self(VectorSlice::Wire(wire_vector_payload(bytes)?)))
    }

    /// Appends the same text as Go `VectorFloat32.String` to reusable storage.
    pub fn append_text(&self, output: &mut String) {
        self.0
            .write_text(output)
            .expect("writing to a String succeeds");
    }

    /// Appends Go vector text to a byte buffer, including one with non-UTF-8 data.
    pub fn append_text_bytes(&self, output: &mut Vec<u8>) {
        let dimensions = self.0.len();
        if dimensions <= 4 {
            output.reserve(2 + dimensions * 8);
        }
        self.0
            .write_text(&mut VectorTextBytes(output))
            .expect("writing to a Vec succeeds");
    }

    /// Lexicographically compares borrowed elements, then dimensions.
    #[inline]
    pub fn compare(&self, other: &Self) -> Ordering {
        match self.0.try_fold_pair(other.0, (), |(), left, right| {
            // Unordered elements compare equal here, matching Go's two tests.
            if left < right {
                ControlFlow::Break(Ordering::Less)
            } else if left > right {
                ControlFlow::Break(Ordering::Greater)
            } else {
                ControlFlow::Continue(())
            }
        }) {
            ControlFlow::Break(ordering) => ordering,
            ControlFlow::Continue(()) => self.0.len().cmp(&other.0.len()),
        }
    }

    /// Compares two serialized vectors without materializing them.
    #[inline]
    pub fn compare_encoded(left: &[u8], right: &[u8]) -> Result<Ordering, VectorError> {
        Ok(compare_wire_vectors(
            wire_vector_payload(left)?,
            wire_vector_payload(right)?,
        ))
    }

    /// Squared L2 distance with source float32 accumulation.
    fn l2_squared_distance(&self, other: &Self) -> Result<f64, VectorError> {
        check_identical_dims(self.0.len(), other.0.len())?;
        let distance = self.0.fold_pair(other.0, 0.0_f32, |distance, left, right| {
            let difference = left - right;
            accumulate_product(distance, difference, difference)
        });
        Ok(f64::from(distance))
    }

    /// Euclidean distance.
    #[inline]
    pub fn l2_distance(&self, other: &Self) -> Result<f64, VectorError> {
        Ok(self.l2_squared_distance(other)?.sqrt())
    }

    /// Inner product with source float32 accumulation.
    fn inner_product(&self, other: &Self) -> Result<f64, VectorError> {
        check_identical_dims(self.0.len(), other.0.len())?;
        Ok(f64::from(self.0.fold_pair(
            other.0,
            0.0_f32,
            accumulate_product,
        )))
    }

    /// Negative inner product.
    #[inline]
    pub fn negative_inner_product(&self, other: &Self) -> Result<f64, VectorError> {
        Ok(-self.inner_product(other)?)
    }

    /// Cosine distance with source clamping and zero-vector NaN behavior.
    #[inline]
    pub fn cosine_distance(&self, other: &Self) -> Result<f64, VectorError> {
        check_identical_dims(self.0.len(), other.0.len())?;
        let (dot, left_norm, right_norm) = self.0.fold_pair(
            other.0,
            (0.0_f32, 0.0_f32, 0.0_f32),
            |(dot, left_norm, right_norm), left, right| {
                (
                    accumulate_product(dot, left, right),
                    accumulate_product(left_norm, left, left),
                    accumulate_product(right_norm, right, right),
                )
            },
        );
        let similarity = f64::from(dot) / (f64::from(left_norm) * f64::from(right_norm)).sqrt();
        if similarity.is_nan() {
            return Ok(f64::NAN);
        }
        Ok(1.0 - similarity.clamp(-1.0, 1.0))
    }

    /// Manhattan distance with source float32 accumulation.
    #[inline]
    pub fn l1_distance(&self, other: &Self) -> Result<f64, VectorError> {
        check_identical_dims(self.0.len(), other.0.len())?;
        Ok(f64::from(self.0.fold_pair(
            other.0,
            0.0_f32,
            |distance, left, right| distance + (left - right).abs(),
        )))
    }

    /// L2 norm with the source's intentional float64 accumulation.
    #[inline]
    pub fn l2_norm(&self) -> f64 {
        self.0
            .fold(0.0, |norm, value| norm + f64::from(value).powi(2))
            .sqrt()
    }
}

fn check_identical_dims(left: usize, right: usize) -> Result<(), VectorError> {
    if left == right {
        Ok(())
    } else {
        Err(VectorError::new(format!(
            "vectors have different dimensions: {left} and {right}"
        )))
    }
}

#[inline]
fn accumulate_product(sum: f32, left: f32, right: f32) -> f32 {
    // Go's arm64 compiler fuses the product and addition in these loops.
    if cfg!(target_arch = "aarch64") {
        left.mul_add(right, sum)
    } else {
        sum + left * right
    }
}

/// Checks source dimension bounds.
pub fn check_vector_dim_valid(dimensions: isize) -> Result<(), VectorError> {
    if dimensions < 0 {
        return Err(VectorError::new(
            "dimensions for type vector must be at least 0",
        ));
    }
    if dimensions as usize > MAX_VECTOR_DIMENSION {
        return Err(VectorError::new(format!(
            "vector cannot have more than {MAX_VECTOR_DIMENSION} dimensions"
        )));
    }
    Ok(())
}

fn parse_vector_text(text: &str) -> Result<VectorFloat32, VectorError> {
    let invalid = || VectorError::new(format!("Invalid vector text: {text}"));
    let start = skip_vector_space(text);
    let Some(array) = start.strip_prefix('[') else {
        if let Some(tail) = start.strip_prefix("null") {
            if text.trim() != "null" && valid_vector_tail(tail) {
                return Ok(VectorFloat32::default());
            }
        }
        return Err(invalid());
    };
    let mut input = skip_vector_space(array);
    let capacity = if input.len() >= 128 {
        (memchr::memchr_iter(b',', input.as_bytes()).count() + 1).min(MAX_VECTOR_DIMENSION)
    } else {
        0
    };
    let mut elements = SmallVec::<[f32; INLINE_VECTOR_ELEMENTS]>::with_capacity(capacity);
    let mut dimensions = 0;
    let tail = if let Some(tail) = input.strip_prefix(']') {
        tail
    } else {
        loop {
            let (value, tail) = read_vector_number(input).ok_or_else(invalid)?;
            if value.abs() > f64::from(f32::MAX) {
                return Err(VectorError::new(format!(
                    "value {} out of range for float32",
                    format_go_exponent(value)
                )));
            }
            if dimensions < MAX_VECTOR_DIMENSION {
                elements.push(value as f32);
            }
            dimensions += 1;
            let tail = skip_vector_space(tail);
            match tail.as_bytes().first() {
                Some(b',') => input = skip_vector_space(&tail[1..]),
                Some(b']') => break &tail[1..],
                _ => return Err(invalid()),
            }
        }
    };
    if !valid_vector_tail(tail) {
        return Err(invalid());
    }
    check_vector_dim_valid(dimensions as isize)?;
    if elements.spilled() {
        return Ok(VectorFloat32::from_elements(elements.into_vec()));
    }
    let mut values = [0.0; INLINE_VECTOR_ELEMENTS];
    values[..elements.len()].copy_from_slice(&elements);
    Ok(VectorFloat32 {
        elements: VectorElements::Inline {
            values,
            len: dimensions as u8,
        },
    })
}

#[inline]
fn skip_vector_space(input: &str) -> &str {
    let count = input
        .as_bytes()
        .iter()
        .take_while(|&&byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
        .count();
    &input[count..]
}

fn valid_vector_tail(tail: &str) -> bool {
    // jsoniter's trailing Skip consumes one unexpected byte. Go's TrimSpace
    // also treats vertical tab and form feed as whitespace.
    matches!(skip_vector_space(tail).as_bytes(), [] | [b'\x0b' | b'\x0c', ..])
}

#[inline]
fn read_vector_number(input: &str) -> Option<(f64, &str)> {
    const POW10: [f64; 7] = [1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0];
    let magnitude = input.strip_prefix('-').unwrap_or(input);
    let negative = magnitude.len() != input.len();
    let bytes = magnitude.as_bytes();
    let first = *bytes.first()?;
    if first == b'.' || (first == b'0' && bytes.get(1).is_some_and(u8::is_ascii_digit)) {
        return None;
    }
    if !first.is_ascii_digit() {
        return read_vector_number_slow(magnitude, negative, 0, false);
    }

    let mut value = u64::from(first - b'0');
    let mut len = 1;
    while let Some(digit) = bytes
        .get(len)
        .map(|byte| byte.wrapping_sub(b'0'))
        .filter(|&digit| digit < 10)
    {
        if len == 15 {
            return read_vector_number_slow(magnitude, negative, len, false);
        }
        value = value * 10 + u64::from(digit);
        len += 1;
    }
    let mut scale = 0;
    if bytes.get(len) == Some(&b'.') {
        len += 1;
        let start = len;
        while let Some(digit) = bytes
            .get(len)
            .map(|byte| byte.wrapping_sub(b'0'))
            .filter(|&digit| digit < 10)
        {
            if len == 16 || len - start == 6 {
                return read_vector_number_slow(magnitude, negative, len, true);
            }
            value = value * 10 + u64::from(digit);
            len += 1;
        }
        scale = len - start;
        if scale == 0 {
            return None;
        }
    }
    if !matches!(bytes.get(len), Some(b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r')) {
        return read_vector_number_slow(magnitude, negative, len, scale != 0);
    }
    let value = if scale == 0 {
        value as f64
    } else {
        value as f64 / POW10[scale]
    };
    Some((if negative { -value } else { value }, &magnitude[len..]))
}

#[cold]
fn read_vector_number_slow(
    magnitude: &str,
    negative: bool,
    checked: usize,
    checked_fraction: bool,
) -> Option<(f64, &str)> {
    let rest = &magnitude.as_bytes()[checked..];
    let len = checked + rest
        .iter()
        .position(|byte| !matches!(byte, b'0'..=b'9' | b'+' | b'-' | b'.' | b'e' | b'E'))?;
    let number = &magnitude[..len];
    if (checked == 0 && number.starts_with('-'))
        || (!checked_fraction && number[checked..].split_once('.').is_some_and(|(_, fraction)| {
            !fraction.as_bytes().first().is_some_and(u8::is_ascii_digit)
        }))
    {
        return None;
    }
    let value = number.parse::<f64>().ok()?;
    value.is_finite().then(|| (if negative { -value } else { value }, &magnitude[len..]))
}

fn format_go_exponent(value: f64) -> String {
    let scientific = format!("{value:e}");
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        return scientific;
    };
    let exponent: i32 = exponent.parse().expect("Rust exponent is numeric");
    format!("{mantissa}e{exponent:+}")
}

fn write_vector_text(
    elements: impl Iterator<Item = f32>,
    output: &mut impl fmt::Write,
) -> fmt::Result {
    output.write_char('[')?;
    let mut buffer = ryu::Buffer::new();
    for (index, value) in elements.enumerate() {
        if index != 0 {
            output.write_char(',')?;
        }
        write_f32_fixed_shortest(value, &mut buffer, output)?;
    }
    output.write_char(']')
}

fn write_f32_fixed_shortest(
    value: f32,
    buffer: &mut ryu::Buffer,
    formatter: &mut impl fmt::Write,
) -> fmt::Result {
    let magnitude = value.abs();
    if magnitude == 0.0 {
        return formatter.write_str(if value.is_sign_negative() { "-0" } else { "0" });
    }
    if (0.001..1024.0).contains(&magnitude) {
        let thousandths = (magnitude * 1000.0).round() as u32;
        // At this magnitude the spacing between float32 values is smaller
        // than 0.001. A thousandth that round-trips is the unique shortest
        // candidate after removing its trailing zeros.
        if thousandths as f32 / 1000.0 == magnitude {
            return write_f32_thousandths(thousandths, value.is_sign_negative(), formatter);
        }
    }
    if !value.is_finite() {
        return formatter.write_str(if value.is_nan() {
            "NaN"
        } else if value.is_sign_negative() {
            "-Inf"
        } else {
            "+Inf"
        });
    }
    let shortest = buffer.format_finite(value);
    let Some((mantissa, exponent)) = shortest.split_once('e') else {
        return formatter.write_str(shortest.strip_suffix(".0").unwrap_or(shortest));
    };
    // Ryu uses a one-digit significand before the decimal point in exponent
    // notation. A float32 needs at most 44 leading zeros in fixed notation.
    const ZEROS: &str = "000000000000000000000000000000000000000000000000";
    let exponent: i32 = exponent.parse().expect("Ryu exponent is numeric");
    let mantissa = if let Some(unsigned) = mantissa.strip_prefix('-') {
        formatter.write_str("-")?;
        unsigned
    } else {
        mantissa
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if exponent < 0 {
        formatter.write_str("0.")?;
        formatter.write_str(&ZEROS[..(-exponent - 1) as usize])?;
        formatter.write_str(integer)?;
        formatter.write_str(fraction)
    } else {
        let exponent = exponent as usize;
        let split = exponent.min(fraction.len());
        let (whole, fractional) = fraction.split_at(split);
        formatter.write_str(integer)?;
        formatter.write_str(whole)?;
        if fractional.is_empty() {
            formatter.write_str(&ZEROS[..exponent - split])
        } else {
            formatter.write_str(".")?;
            formatter.write_str(fractional)
        }
    }
}

fn write_f32_thousandths(
    thousandths: u32,
    negative: bool,
    output: &mut impl fmt::Write,
) -> fmt::Result {
    let mut digits = itoa::Buffer::new();
    let whole = digits.format(thousandths / 1000).as_bytes();
    let fraction = thousandths % 1000;
    let mut text = [0; 9];
    text[0] = b'-';
    let start = usize::from(negative);
    let mut end = start + whole.len();
    text[start..end].copy_from_slice(whole);
    if fraction != 0 {
        text[end] = b'.';
        text[end + 1] = b'0' + (fraction / 100) as u8;
        text[end + 2] = b'0' + (fraction / 10 % 10) as u8;
        text[end + 3] = b'0' + (fraction % 10) as u8;
        end += 4;
        while text[end - 1] == b'0' {
            end -= 1;
        }
    }
    output.write_str(std::str::from_utf8(&text[..end]).expect("decimal digits are ASCII"))
}

fn write_f32_general_two_digits(value: f32, output: &mut String) {
    if value.is_nan() {
        output.push_str("NaN");
        return;
    }
    if value.is_infinite() {
        output.push_str(if value.is_sign_negative() {
            "-Inf"
        } else {
            "+Inf"
        });
        return;
    }
    if value.is_sign_negative() {
        output.push('-');
    }
    if value == 0.0 {
        output.push('0');
        return;
    }

    // Go chooses the notation after rounding to two significant digits.
    let mut buffer = [0_u8; 16];
    let used = {
        use std::io::Write as _;
        let mut remaining = &mut buffer[..];
        write!(remaining, "{:.1e}", value.abs()).expect("f32 scientific notation fits");
        16 - remaining.len()
    };
    let scientific = std::str::from_utf8(&buffer[..used]).expect("scientific notation is ASCII");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("scientific format has exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("scientific format has a numeric exponent");
    let digits = mantissa.as_bytes();
    let first = char::from(digits[0]);
    let second = char::from(digits[2]);
    match exponent {
        -4..=-1 => {
            output.push_str(&"0.000"[..(1 - exponent) as usize]);
            output.push(first);
            if second != '0' {
                output.push(second);
            }
        }
        1 => {
            output.push(first);
            output.push(second);
        }
        _ => {
            output.push(first);
            if second != '0' {
                output.push('.');
                output.push(second);
            }
            if exponent != 0 {
                use std::fmt::Write as _;
                write!(output, "e{exponent:+03}").expect("writing to a string");
            }
        }
    }
}

/// Returns the number of bytes occupied by the first serialized vector.
#[inline]
pub fn peek_vector_float32(bytes: &[u8]) -> Result<usize, VectorError> {
    let Some(header) = bytes.first_chunk::<4>() else {
        return Err(vector_size_error(bytes.len(), None));
    };
    let dimensions = u32::from_le_bytes(*header);
    if dimensions >= u32::MAX / 4 {
        return Err(vector_size_overflow());
    }
    let expected = (dimensions as usize + 1) * 4;
    if bytes.len() < expected {
        return Err(vector_size_error(bytes.len(), Some(expected)));
    }
    Ok(expected)
}

#[cold]
fn vector_size_error(length: usize, expected: Option<usize>) -> VectorError {
    match expected {
        None => VectorError::new(format!("bad VectorFloat32 value header (len={length})")),
        Some(expected) => VectorError::new(format!(
            "bad VectorFloat32 value (len={length}, expected={expected})"
        )),
    }
}

#[cold]
fn vector_size_overflow() -> VectorError {
    VectorError::new("bad VectorFloat32 value size overflow")
}

/// Deserializes the first vector and returns it with the unconsumed suffix.
#[inline]
pub fn deserialize_vector_float32(bytes: &[u8]) -> Result<(VectorFloat32, &[u8]), VectorError> {
    let length = peek_vector_float32(bytes)?;
    let payload = &bytes[4..length];
    if payload.len() <= INLINE_VECTOR_ELEMENTS * 4 {
        let mut values = [0.0; INLINE_VECTOR_ELEMENTS];
        let words = payload.as_chunks::<4>().0;
        for (value, &word) in values.iter_mut().zip(words) {
            *value = f32::from_le_bytes(word);
        }
        let elements = VectorElements::Inline {
            values,
            len: words.len() as u8,
        };
        return Ok((VectorFloat32 { elements }, &bytes[length..]));
    }
    Ok((
        VectorFloat32::from_elements(decode_vector_elements(payload)),
        &bytes[length..],
    ))
}

fn decode_vector_elements(bytes: &[u8]) -> Vec<f32> {
    wire_elements(bytes).collect()
}

fn wire_elements(bytes: &[u8]) -> impl ExactSizeIterator<Item = f32> + '_ {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .copied()
        .map(f32::from_le_bytes)
}

#[cfg(all(test, target_endian = "little"))]
mod tests {
    use super::*;

    #[test]
    fn wire_comparison_scans_past_unordered_elements_in_a_different_block() {
        let mut left = VectorFloat32::must_create(vec![1.0; 35]);
        let mut right = left.clone();
        left.elements_mut()[7] = f32::NAN;
        left.elements_mut()[5] = -0.0;
        right.elements_mut()[5] = 0.0;
        left.elements_mut()[23] = 7.0;
        right.elements_mut()[34] = 9.0;
        let left_bytes = left.serialize();
        let right_bytes = right.serialize();
        assert_eq!(
            VectorFloat32Ref::compare_encoded(&left_bytes, &right_bytes).unwrap(),
            Ordering::Greater,
        );
        assert_eq!(
            VectorFloat32Ref::compare_encoded(&right_bytes, &left_bytes).unwrap(),
            Ordering::Less,
        );
    }

    #[test]
    fn wire_operations_defer_alignment_until_a_native_slice_is_requested() {
        let native = VectorFloat32::must_create(vec![-0.0, 2.5, -3.0]);
        let bytes = native.serialize();
        let alignment = align_of::<f32>();
        let mut packed = Vec::<u8>::with_capacity(bytes.len() + alignment);
        let offset = packed.as_ptr().align_offset(alignment) + 1;
        packed.resize(offset, 0);
        packed.extend_from_slice(&bytes);
        let column = Arc::new(SharedVectorBytes::new(Bytes::from(packed).slice(offset..)));
        let wire = column.read(0..bytes.len()).unwrap();
        let mut detached = wire.clone();
        detached.elements_mut()[1] = 5.0;

        assert_eq!(wire.len(), 3);
        assert_eq!(wire.serialize(), bytes);
        assert_eq!(wire.l2_distance(&native).unwrap(), 0.0);
        assert_eq!(native.inner_product(&wire).unwrap(), 15.25);
        assert_eq!(wire.l1_distance(&detached).unwrap(), 2.5);
        assert!(column.aligned.get().is_none());

        let second = column.read(0..bytes.len()).unwrap();
        assert_eq!(wire.elements()[0].to_bits(), (-0.0_f32).to_bits());
        assert_eq!(wire.elements().as_ptr(), second.elements().as_ptr());
        assert!(column.aligned.get().is_some());
    }

    #[test]
    fn comparing_and_formatting_packed_vectors_leave_the_column_unaligned() {
        let mut native = VectorFloat32::must_create(vec![0.0, 10.0, 99.99, 0.00009999, -0.0, 42.0]);
        let mut packed = vec![0];
        native.elements_mut()[0] = f32::NAN;
        native.serialize_to(&mut packed);
        native.elements_mut()[0] = 17.0;
        native.serialize_to(&mut packed);
        let cell_size = (packed.len() - 1) / 2;
        let column = Arc::new(SharedVectorBytes::new(Bytes::from(packed).slice(1..)));
        let first = column.read(0..cell_size).unwrap();
        let second = column.read(cell_size..2 * cell_size).unwrap();
        let shorter = VectorFloat32::must_create(vec![0.0]);

        assert_eq!(first.compare(&native), Ordering::Equal);
        assert_eq!(native.compare(&first), Ordering::Equal);
        assert_eq!(first.compare(&second), Ordering::Equal);
        assert_eq!(first.compare(&shorter), Ordering::Greater);
        native.elements_mut()[5] = 43.0;
        assert_eq!(first.compare(&native), Ordering::Less);
        assert_eq!(
            first.truncated_string(),
            "[NaN,10,1e+02,0.0001,-0,(1 more)...]"
        );
        let expected = "[NaN,10,99.99,0.00009999,-0,42]";
        assert_eq!(first.to_string(), expected);
        let mut output = String::from("vector: ");
        VectorFloat32Ref::from_encoded(&column.bytes()[..cell_size])
            .unwrap()
            .append_text(&mut output);
        assert_eq!(output.strip_prefix("vector: "), Some(expected));
        assert!(column.aligned.get().is_none());
    }

    #[test]
    fn first_native_vector_slice_aligns_only_its_cell() {
        let mut packed = vec![0];
        for value in 0..8 {
            VectorFloat32::must_create(vec![value as f32; 16]).serialize_to(&mut packed);
        }
        let cell_len = (packed.len() - 1) / 8;
        let column = Arc::new(SharedVectorBytes::new(Bytes::from(packed).slice(1..)));
        let first = column.read(3 * cell_len..4 * cell_len).unwrap();
        let first_slice = first.elements();
        assert_eq!(first_slice, &[3.0; 16]);
        let Some(AlignedVectorElements::First(aligned)) = column.aligned.get() else {
            panic!("only the requested cell should have an aligned copy");
        };
        assert!(aligned.whole.get().is_none());

        let second = column.read(6 * cell_len..7 * cell_len).unwrap();
        assert_eq!(second.elements(), &[6.0; 16]);
        assert!(aligned.whole.get().is_some());
        assert_eq!(first.elements().as_ptr(), first_slice.as_ptr());
    }

    #[test]
    fn alignment_buffers_are_refilled_from_the_new_frame() {
        const CELL_BYTES: usize = 4 + 16 * 4;
        let frame = |seed| {
            let mut bytes = vec![0];
            for row in 0..8 {
                VectorFloat32::must_create(vec![(seed + row) as f32; 16])
                    .serialize_to(&mut bytes);
            }
            Bytes::from(bytes).slice(1..)
        };
        let payload = |row: usize| row * CELL_BYTES + 4..(row + 1) * CELL_BYTES;
        let mut column = SharedVectorBytes::new(frame(0));
        assert_eq!(column.elements(payload(2)), &[2.0; 16]);
        assert_eq!(column.elements(payload(3)), &[3.0; 16]);

        column.replace_bytes(frame(10));
        let spare = column.aligned_spare.get_mut().unwrap();
        assert_eq!(spare.first.len(), 16);
        assert_eq!(spare.whole.len(), 8 * CELL_BYTES / 4);
        assert_eq!(column.elements(payload(5)), &[15.0; 16]);
        assert_eq!(column.elements(payload(2)), &[12.0; 16]);
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn alternating_frames_reuse_their_own_aligned_slices() {
        let frame = |value| {
            let mut bytes = vec![0];
            VectorFloat32::must_create(vec![value; 16]).serialize_to(&mut bytes);
            Bytes::from(bytes).slice(1..)
        };
        let aligned_ptr = |column: &SharedVectorBytes, value| {
            let elements = column.elements(4..column.bytes().len());
            assert_eq!(elements, &[value; 16]);
            elements.as_ptr()
        };
        let first = frame(1.0);
        let second = frame(2.0);
        let mut column = SharedVectorBytes::new(first.clone());
        aligned_ptr(&column, 1.0);

        column.replace_bytes(second.clone());
        let second_ptr = aligned_ptr(&column, 2.0);
        column.replace_bytes(first.clone());
        let first_ptr = aligned_ptr(&column, 1.0);
        assert_ne!(first_ptr, second_ptr);

        column.replace_bytes(second);
        assert_eq!(aligned_ptr(&column, 2.0), second_ptr);
        column.replace_bytes(first);
        assert_eq!(aligned_ptr(&column, 1.0), first_ptr);

        column.replace_bytes(frame(3.0));
        aligned_ptr(&column, 3.0);
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn rotating_frames_reuse_alignments_and_release_a_stale_history() {
        let frame = |value: usize| {
            let mut bytes = vec![0];
            VectorFloat32::must_create(vec![value as f32; 16]).serialize_to(&mut bytes);
            Bytes::from(bytes).slice(1..)
        };
        let frames = [frame(0), frame(1), frame(2), frame(3)];
        let mut column = SharedVectorBytes::new(frames[0].clone());
        let mut addresses = [None; 4];
        for cycle in 0..4 {
            for (i, bytes) in frames.iter().enumerate() {
                if cycle != 0 || i != 0 {
                    column.replace_bytes(bytes.clone());
                }
                let values = column.elements(4..bytes.len());
                assert_eq!(values, &[i as f32; 16]);
                if cycle == 1 {
                    addresses[i] = Some(values.as_ptr());
                } else if cycle > 1 {
                    assert_eq!(addresses[i], Some(values.as_ptr()));
                }
            }
            if cycle == 0 {
                assert!(column.retained.is_none());
            }
        }

        // Keep the distinct inputs live so they cannot reuse an old address.
        let unrelated = [frame(10), frame(11), frame(12), frame(13)];
        for (i, bytes) in unrelated.iter().enumerate() {
            column.replace_bytes(bytes.clone());
            assert_eq!(column.elements(4..bytes.len()), &[(10 + i) as f32; 16]);
        }
        assert!(column.retained.is_none());
    }

    #[test]
    fn packed_arithmetic_and_equality_leave_the_column_unaligned() {
        let mut native = VectorFloat32::must_create(vec![1.5; 19]);
        native.elements_mut()[0] = -0.0;
        native.elements_mut()[7] = -4.0;
        let mut exceptional = native.clone();
        exceptional.elements_mut()[1] = f32::MAX;
        exceptional.elements_mut()[18] = f32::NAN;

        let mut packed = vec![0];
        native.serialize_to(&mut packed);
        exceptional.serialize_to(&mut packed);
        let cell_size = native.serialized_size();
        let column = Arc::new(SharedVectorBytes::new(Bytes::from(packed).slice(1..)));
        let value = column.read(0..cell_size).unwrap();
        let error = column.read(cell_size..2 * cell_size).unwrap();

        let mut positive_zero = native.clone();
        positive_zero.elements_mut()[0] = 0.0;
        assert_eq!(value, positive_zero);
        assert_ne!(error, error);
        assert_eq!(value.add(&native).unwrap(), native.add(&native).unwrap());
        assert_eq!(
            native.sub(&value).unwrap().serialize(),
            native.sub(&native).unwrap().serialize()
        );
        assert_eq!(value.mul(&value).unwrap(), native.mul(&native).unwrap());
        assert_eq!(
            error.add(&error).unwrap_err().to_string(),
            "value out of range: overflow"
        );
        assert_eq!(
            error.sub(&error).unwrap_err().to_string(),
            "value out of range: NaN"
        );
        assert!(column.aligned.get().is_none());
    }
}
