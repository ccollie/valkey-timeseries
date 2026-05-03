use crate::common::Sample;
use crate::series::chunks::xor2::chunk::read_st_header;
use std::error::Error;
use std::io;
use crate::series::chunks::bstream_reader::BStreamReader;

pub struct XOR2Iterator<'a> {
    br: BStreamReader<'a>,
    num_total: u16,
    num_read: u16,
    first_st_known: bool,
    first_st_change_on: u8,
    leading: u8,
    trailing: u8,
    st: i64,
    t: i64,
    val: f64,
    t_delta: u64,
    st_diff: i64,
    err: Option<io::Error>,
    baseline_v: f64,
}

impl<'a> XOR2Iterator<'a> {
    pub(super) fn new(buf: &'a [u8]) -> Self {
        Self {
            br: BStreamReader::new(buf),
            num_total: 0,
            num_read: 0,
            first_st_known: false,
            first_st_change_on: 0,
            leading: 0,
            trailing: 0,
            st: 0,
            t: 0,
            val: 0.0,
            t_delta: 0,
            st_diff: 0,
            err: None,
            baseline_v: 0.0,
        }
    }

    pub(super) fn reset(&mut self, b: &'a [u8]) {
        let st_header_start = crate::series::chunks::xor2::chunk::CHUNK_HEADER_SIZE;
        let data_start = st_header_start + 1;

        if b.len() > data_start {
            self.br = BStreamReader::new(&b[data_start..]);
        }

        if b.len() >= 2 {
            self.num_total = u16::from_be_bytes([b[0], b[1]]);
        }

        if b.len() > st_header_start {
            let (first_st_known, first_st_change_on) = read_st_header(&b[st_header_start..st_header_start + 1]);
            self.first_st_known = first_st_known;
            self.first_st_change_on = first_st_change_on;
        }

        self.num_read = 0;
        self.st = 0;
        self.t = 0;
        self.val = 0.0;
        self.leading = 0;
        self.trailing = 0;
        self.t_delta = 0;
        self.st_diff = 0;
        self.baseline_v = 0.0;
        self.err = None;
    }

    fn read_dod(&mut self, w: u8) -> io::Result<()> {
        let b = if self.br.valid >= w {
            self.br.valid -= w;
            (self.br.buffer >> self.br.valid) & ((1u64 << w) - 1)
        } else {
            self.br.read_bits(w)?
        };

        let dod = if w < 64 && b >= (1u64 << (w - 1)) {
            (b as i64) - (1i64 << w)
        } else {
            b as i64
        };

        self.t_delta = (self.t_delta as i64 + dod) as u64;
        self.t += self.t_delta as i64;
        Ok(())
    }

    fn decode_value(&mut self) -> io::Result<()> {
        if self.br.valid >= 3 {
            let ctrl = (self.br.buffer >> (self.br.valid - 3)) & 0x7;
            if ctrl & 0x4 == 0 {
                self.br.valid -= 1;
                self.val = self.baseline_v;
                return Ok(());
            }
            if ctrl & 0x6 == 0x4 {
                self.br.valid -= 2;
                let sz = 64 - self.leading - self.trailing;
                let value_bits = if self.br.valid >= sz {
                    self.br.valid -= sz;
                    (self.br.buffer >> self.br.valid) & ((1u64 << sz) - 1)
                } else {
                    self.br.read_bits(sz)?
                };
                let mut v_bits = self.baseline_v.to_bits();
                v_bits ^= value_bits << self.trailing;
                self.val = f64::from_bits(v_bits);
                self.baseline_v = self.val;
                return Ok(());
            }
            self.br.valid -= 3;
            if ctrl == 0x6 {
                return self.decode_new_leading_trailing();
            }
            self.val = f64::from_bits(crate::series::chunks::xor2::chunk::STALE_NAN);
            return Ok(());
        }

        // Slow path
        let bit = self.br.read_bit()?;
        if !bit {
            self.val = self.baseline_v;
            return Ok(());
        }

        let bit2 = self.br.read_bit()?;
        if !bit2 {
            let sz = 64 - self.leading - self.trailing;
            let value_bits = if self.br.valid >= sz {
                self.br.valid -= sz;
                (self.br.buffer >> self.br.valid) & ((1u64 << sz) - 1)
            } else {
                self.br.read_bits(sz)?
            };
            let mut v_bits = self.baseline_v.to_bits();
            v_bits ^= value_bits << self.trailing;
            self.val = f64::from_bits(v_bits);
            self.baseline_v = self.val;
            return Ok(());
        }

        let bit3 = self.br.read_bit()?;
        if !bit3 {
            return self.decode_new_leading_trailing();
        }

        self.val = f64::from_bits(crate::series::chunks::xor2::chunk::STALE_NAN);
        Ok(())
    }

    fn decode_value_known_non_zero(&mut self) -> io::Result<()> {
        let sz = 64 - self.leading - self.trailing ;

        if self.br.valid > sz {
            let ctrl_bit = (self.br.buffer >> (self.br.valid - 1)) & 1;
            if ctrl_bit == 0 {
                self.br.valid -= 1 + sz;
                let value_bits = (self.br.buffer >> self.br.valid) & ((1u64 << sz) - 1);
                let mut v_bits = self.baseline_v.to_bits();
                v_bits ^= value_bits << self.trailing;
                self.val = f64::from_bits(v_bits);
                self.baseline_v = self.val;
                return Ok(());
            }
            self.br.valid -= 1;
            return self.decode_new_leading_trailing();
        }

        let bit = self.br.read_bit()?;
        if !bit {
            let value_bits = if self.br.valid >= sz {
                self.br.valid -= sz;
                (self.br.buffer >> self.br.valid) & ((1u64 << sz) - 1)
            } else {
                self.br.read_bits(sz)?
            };
            let mut v_bits = self.baseline_v.to_bits();
            v_bits ^= value_bits << self.trailing;
            self.val = f64::from_bits(v_bits);
            self.baseline_v = self.val;
            return Ok(());
        }

        self.decode_new_leading_trailing()
    }

    fn decode_new_leading_trailing(&mut self) -> io::Result<()> {
        let (new_leading, sig_bits) = if self.br.valid >= 11 {
            let val = (self.br.buffer >> (self.br.valid - 11)) & 0x7FF;
            self.br.valid -= 11;
            ((val >> 6) as u8, (val & 0x3F) as u8)
        } else {
            let new_leading = self.br.read_bits(5)? as u8;
            let sig_bits = self.br.read_bits(6)? as u8;
            (new_leading, sig_bits)
        };

        self.leading = new_leading;
        let sigbits = if sig_bits == 0 { 64 } else { sig_bits };
        self.trailing = 64 - self.leading - sigbits ;

        let value_bits = if self.br.valid >= sigbits {
            self.br.valid -= sigbits;
            (self.br.buffer >> self.br.valid) & ((1u64 << sigbits) - 1)
        } else {
            self.br.read_bits(sigbits)?
        };

        let mut v_bits = self.baseline_v.to_bits();
        v_bits ^= value_bits << self.trailing;
        self.val = f64::from_bits(v_bits);
        self.baseline_v = self.val;
        Ok(())
    }

    pub fn seek(&mut self, t: i64) -> Option<Sample> {
        if self.err.is_some() {
            return None;
        }

        while t > self.t || self.num_read == 0 {
            self.next()?;
        }
        Some(Sample {
            timestamp: self.t,
            value: self.val,
        })
    }

    fn at(&self) -> (i64, f64) {
        (self.t, self.val)
    }

    fn at_t(&self) -> i64 {
        self.t
    }

    fn at_st(&self) -> i64 {
        self.st
    }

    fn err(&self) -> Option<&dyn Error> {
        todo!()
    }
}

impl Iterator for XOR2Iterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.err.is_some() || self.num_read == self.num_total {
            return None;
        }

        if self.num_read == 0 {
            let t = match self.br.read_varint() {
                Ok(t) => t,
                Err(e) => {
                    self.err = Some(e);
                    return None;
                }
            };
            let v_bits = match self.br.read_bits(64) {
                Ok(v) => v,
                Err(e) => {
                    self.err = Some(e);
                    return None;
                }
            };
            self.t = t;
            self.val = f64::from_bits(v_bits);
            if !crate::series::chunks::xor2::chunk::is_stale_nan(self.val) {
                self.baseline_v = self.val;
            }

            if self.first_st_known {
                let st_diff = match self.br.read_varint() {
                    Ok(sd) => sd,
                    Err(e) => {
                        self.err = Some(e);
                        return None;
                    }
                };
                self.st = t - st_diff;
            }

            self.num_read += 1;
            return Some(Sample {
                timestamp: self.t,
                value: self.val,
            });
        }

        if self.num_read == 1 {
            let t_delta = match self.br.read_uvarint() {
                Ok(td) => td,
                Err(e) => {
                    self.err = Some(e);
                    return None;
                }
            };
            let prev_t = self.t;
            self.t_delta = t_delta;
            self.t += self.t_delta as i64;

            if let Err(e) = self.decode_value() {
                self.err = Some(e);
                return None;
            }

            if self.first_st_change_on == 1 {
                let sdod = match self.br.read_varint() {
                    Ok(sd) => sd,
                    Err(e) => {
                        self.err = Some(e);
                        return None;
                    }
                };
                self.st_diff = sdod;
                self.st = prev_t - sdod;
            }

            self.num_read += 1;
            return Some(Sample {
                timestamp: self.t,
                value: self.val,
            });
        }

        let prev_t = self.t;
        let saved_num_read = self.num_read;

        let (ctrl, success) = self.br.read_xor2_control_fast();
        let ctrl = if !success {
            match self.br.read_xor2_control() {
                Ok(c) => c,
                Err(e) => {
                    self.err = Some(e);
                    return None;
                }
            }
        } else {
            ctrl
        };

        match ctrl {
            0 => {
                self.t += self.t_delta as i64;
                self.val = self.baseline_v;
            }
            1 => {
                self.t += self.t_delta as i64;
                if let Err(e) = self.decode_value_known_non_zero() {
                    self.err = Some(e);
                    return None;
                }
            }
            2 => {
                if let Err(e) = self.read_dod(13) {
                    self.err = Some(e);
                    return None;
                }
                if let Err(e) = self.decode_value() {
                    self.err = Some(e);
                    return None;
                }
            }
            3 => {
                if let Err(e) = self.read_dod(20) {
                    self.err = Some(e);
                    return None;
                }
                if let Err(e) = self.decode_value() {
                    self.err = Some(e);
                    return None;
                }
            }
            4 => {
                if let Err(e) = self.read_dod(64) {
                    self.err = Some(e);
                    return None;
                }
                if let Err(e) = self.decode_value() {
                    self.err = Some(e);
                    return None;
                }
            }
            _ => {
                self.t += self.t_delta as i64;
                self.val = f64::from_bits(crate::series::chunks::xor2::chunk::STALE_NAN);
            }
        }

        if self.first_st_change_on > 0 && saved_num_read >= self.first_st_change_on as u16 {
            let sdod = match self.br.read_varint() {
                Ok(sd) => sd,
                Err(e) => {
                    self.err = Some(e);
                    return None;
                }
            };
            if saved_num_read == self.first_st_change_on as u16 {
                self.st_diff = sdod;
            } else {
                self.st_diff += sdod;
            }
            self.st = prev_t - self.st_diff;
        }

        self.num_read += 1;

        Some(Sample {
            timestamp: self.t,
            value: self.val,
        })
    }
}