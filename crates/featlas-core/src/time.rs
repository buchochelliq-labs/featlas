//! Exact UTC instants with microsecond precision (SEM-TIME-003).
//!
//! Only `YYYY-MM-DDTHH:MM:SS[.f{1,6}]Z` is accepted. Invalid calendar dates,
//! timezone-less values, offsets other than `Z`, leap seconds and more than six
//! fractional digits are rejected rather than normalized. Instants compare as
//! integers, never as text.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant {
    /// Microseconds since 1970-01-01T00:00:00Z.
    pub micros: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    /// Days since 1970-01-01.
    pub days: i64,
}

pub const MICROS_PER_SECOND: i64 = 1_000_000;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn digits(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut v: i64 = 0;
    for b in s {
        v = v * 10 + i64::from(b - b'0');
    }
    Some(v)
}

fn parse_ymd(b: &[u8]) -> Result<(i64, i64, i64), String> {
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return Err("expected YYYY-MM-DD".into());
    }
    let y = digits(&b[0..4]).ok_or("invalid year")?;
    let m = digits(&b[5..7]).ok_or("invalid month")?;
    let d = digits(&b[8..10]).ok_or("invalid day")?;
    if !(1..=12).contains(&m) {
        return Err(format!("month {m} out of range"));
    }
    if d < 1 || d > days_in_month(y, m) {
        return Err(format!("day {d} invalid for {y:04}-{m:02}"));
    }
    Ok((y, m, d))
}

impl Instant {
    pub fn parse(text: &str) -> Result<Instant, String> {
        let b = text.as_bytes();
        if b.len() < 20 || b[b.len() - 1] != b'Z' {
            return Err(format!("expected UTC timestamp ending in 'Z': {text:?}"));
        }
        let (y, mo, d) = parse_ymd(&b[0..10]).map_err(|e| format!("{e} in {text:?}"))?;
        if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return Err(format!("expected YYYY-MM-DDTHH:MM:SS: {text:?}"));
        }
        let h = digits(&b[11..13]).ok_or_else(|| format!("invalid hour in {text:?}"))?;
        let mi = digits(&b[14..16]).ok_or_else(|| format!("invalid minute in {text:?}"))?;
        let s = digits(&b[17..19]).ok_or_else(|| format!("invalid second in {text:?}"))?;
        if h > 23 || mi > 59 || s > 59 {
            return Err(format!(
                "time of day out of range (leap seconds unsupported): {text:?}"
            ));
        }
        let rest = &b[19..b.len() - 1];
        let frac = if rest.is_empty() {
            0
        } else {
            if rest[0] != b'.' || rest.len() < 2 || rest.len() > 7 {
                return Err(format!("fractional seconds must have 1-6 digits: {text:?}"));
            }
            let f = &rest[1..];
            let v = digits(f).ok_or_else(|| format!("invalid fraction in {text:?}"))?;
            v * 10i64.pow((6 - f.len()) as u32)
        };
        let days = days_from_civil(y, mo, d);
        let secs = days * 86_400 + h * 3600 + mi * 60 + s;
        Ok(Instant {
            micros: secs * MICROS_PER_SECOND + frac,
        })
    }

    pub fn checked_sub_seconds(self, seconds: i64) -> Option<Instant> {
        seconds
            .checked_mul(MICROS_PER_SECOND)
            .and_then(|m| self.micros.checked_sub(m))
            .map(|micros| Instant { micros })
    }

    pub fn checked_add_seconds(self, seconds: i64) -> Option<Instant> {
        seconds
            .checked_mul(MICROS_PER_SECOND)
            .and_then(|m| self.micros.checked_add(m))
            .map(|micros| Instant { micros })
    }
}

impl fmt::Display for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.micros.div_euclid(MICROS_PER_SECOND);
        let frac = self.micros.rem_euclid(MICROS_PER_SECOND);
        let days = secs.div_euclid(86_400);
        let sod = secs.rem_euclid(86_400);
        let (y, m, d) = civil_from_days(days);
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            y,
            m,
            d,
            sod / 3600,
            (sod / 60) % 60,
            sod % 60
        )?;
        if frac != 0 {
            let s = format!("{frac:06}");
            write!(f, ".{}", s.trim_end_matches('0'))?;
        }
        write!(f, "Z")
    }
}

impl Date {
    pub fn parse(text: &str) -> Result<Date, String> {
        let (y, m, d) = parse_ymd(text.as_bytes()).map_err(|e| format!("{e} in {text:?}"))?;
        Ok(Date {
            days: days_from_civil(y, m, d),
        })
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (y, m, d) = civil_from_days(self.days);
        write!(f, "{y:04}-{m:02}-{d:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_compares_as_instants() {
        let a = Instant::parse("2026-06-08T00:00:00Z").unwrap();
        let b = Instant::parse("2026-06-08T00:00:00.000000Z").unwrap();
        let c = Instant::parse("2026-06-07T23:59:59.9Z").unwrap();
        assert_eq!(a, b);
        assert!(c < a);
        assert_eq!(a.micros - c.micros, 100_000);
        assert_eq!(Instant::parse("1970-01-01T00:00:00Z").unwrap().micros, 0);
        assert_eq!(a.to_string(), "2026-06-08T00:00:00Z");
        assert_eq!(c.to_string(), "2026-06-07T23:59:59.9Z");
    }

    #[test]
    fn rejects_invalid_forms() {
        for bad in [
            "2026-02-30T00:00:00Z",
            "2026-06-08T24:00:00Z",
            "2026-06-08T00:00:60Z",
            "2026-06-08T00:00:00",
            "2026-06-08T00:00:00+00:00",
            "2026-06-08 00:00:00Z",
            "2026-06-08T00:00:00.1234567Z",
            "2026-06-08T00:00:00.Z",
        ] {
            assert!(Instant::parse(bad).is_err(), "{bad}");
        }
        assert!(Instant::parse("2024-02-29T00:00:00Z").is_ok());
        assert!(Instant::parse("2100-02-29T00:00:00Z").is_err());
        assert!(Date::parse("2026-13-01").is_err());
    }

    #[test]
    fn round_trips_before_epoch() {
        let t = Instant::parse("1969-12-31T23:59:59.5Z").unwrap();
        assert_eq!(t.micros, -500_000);
        assert_eq!(t.to_string(), "1969-12-31T23:59:59.5Z");
    }
}
