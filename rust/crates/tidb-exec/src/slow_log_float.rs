fn normalize_exponent(mantissa: &str, exponent: i32) -> String {
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
}

pub(crate) fn format_go_float64(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f64::INFINITY {
        return "+Inf".to_owned();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".to_owned();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust scientific float contains exponent");
    let exponent: i32 = exponent.parse().expect("Rust float exponent is numeric");
    if !(-4..6).contains(&exponent) {
        normalize_exponent(mantissa, exponent)
    } else {
        value.to_string()
    }
}
