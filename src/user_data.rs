use std::convert::TryFrom;
use std::fmt;
use std::os::unix::io::RawFd;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum IOError {
    NegativeFd(RawFd),
}

const TAG_SHIFT: u64 = 32;
const LOW_MASK: u64 = 0xFFFF_FFFF;

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct UserData(u64);

impl std::fmt::Display for UserData {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        let op = self.op().expect("op unwrap in Display");
        let fd = self.fd();
        write!(f, "op={} fd={}", op, fd)
    }
}

impl UserData {
    #[inline]
    pub fn new(op: Op, fd: RawFd) -> Self {
        Self::from_raw_parts(op.to_u32(), fd as u32)
    }

    /// Cross-platform constructor where you pass the bottom 32 bits directly.
    #[inline]
    pub const fn from_raw_parts(op_u32: u32, low_u32: u32) -> Self {
        let hi = (op_u32 as u64) << TAG_SHIFT;
        let lo = low_u32 as u64;
        Self(hi | lo)
    }

    /// Extract raw parts as `(op_u32, low_u32)`.
    #[inline]
    pub const fn to_raw_parts(self) -> (u32, u32) {
        let hi = (self.0 >> TAG_SHIFT) as u32;
        let lo = (self.0 & LOW_MASK) as u32;
        (hi, lo)
    }

    /// Try to decode the tag as `Op`.
    /// Returns `Ok(Op)` if it matches, or `Err(raw_value)` if unknown.
    #[inline]
    pub fn op(self) -> Result<Op, u32> {
        match (self.0 >> TAG_SHIFT) as u32 {
            v if v == Op::Accept as u32 => Ok(Op::Accept),
            v if v == Op::Timeout as u32 => Ok(Op::Timeout),
            v if v == Op::Recv as u32 => Ok(Op::Recv),
            v if v == Op::Send as u32 => Ok(Op::Send),
            v => Err(v),
        }
    }

    /// Get the bottom 32 bits (as `u32`).
    #[inline]
    pub const fn low_u32(self) -> u32 {
        (self.0 & LOW_MASK) as u32
    }

    /// On Unix, interpret the bottom bits as a `RawFd` (i32).
    #[cfg(unix)]
    #[inline]
    pub fn fd(self) -> RawFd {
        self.low_u32() as RawFd
    }

    /// The packed `u64`.
    #[inline]
    pub const fn into_u64(self) -> u64 {
        self.0
    }
}

impl From<UserData> for u64 {
    #[inline]
    fn from(p: UserData) -> u64 {
        p.0
    }
}

impl From<(u32, u32)> for UserData {
    /// Unchecked: lets you pack arbitrary 32-bit values.
    #[inline]
    fn from(parts: (u32, u32)) -> Self {
        Self::from_raw_parts(parts.0, parts.1)
    }
}

impl TryFrom<u64> for UserData {
    type Error = ();

    /// Always succeeds (all bit patterns are valid),
    /// but provided for parity with integer conversions.
    #[inline]
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        Ok(UserData(v))
    }
}

impl fmt::Debug for UserData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (k, lo) = self.to_raw_parts();
        match self.op() {
            Ok(op) => write!(
                f,
                "UserData {{ op: {:?} ({:#010x}), low: {:#010x}, u64: {:#018x} }}",
                op, k, lo, self.0
            ),
            Err(raw) => write!(
                f,
                "UserData {{ op: <unknown> ({:#010x}), low: {:#010x}, u64: {:#018x} }}",
                raw, lo, self.0
            ),
        }
    }
}

#[repr(u32)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Op {
    Accept = 0x0001,
    Timeout = 0x0002,
    Recv = 0x0004,
    Send = 0x0008,
    Other = 0xFFFF,
}

impl std::fmt::Display for Op {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        match self {
            Op::Accept => write!(f, "Op(Accept)"),
            Op::Timeout => write!(f, "Op(Timeout)"),
            Op::Recv => write!(f, "Op(Recv)"),
            Op::Send => write!(f, "Op(Send)"),
            Op::Other => write!(f, "Op(Other)"),
        }
    }
}

impl Op {
    #[inline]
    const fn to_u32(self) -> u32 {
        self as u32
    }
}
