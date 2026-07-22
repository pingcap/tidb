// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Locale-aware `FORMAT()` number grouping and separators.

use crate::charset::is_unicode_decimal_digit;

/// Separator/grouping rules for a MySQL locale.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocaleFormatStyle {
    /// Thousands separator.
    pub thousands_separator: &'static str,
    /// Decimal point.
    pub decimal_point: &'static str,
    /// Whether grouping is 3 digits then repeated 2-digit groups.
    pub indian_grouping: bool,
}
const COMMA_DOT: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: ",",
    decimal_point: ".",
    indian_grouping: false,
};
const DOT_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: ".",
    decimal_point: ",",
    indian_grouping: false,
};
const SPACE_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: " ",
    decimal_point: ",",
    indian_grouping: false,
};
const NONE_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "",
    decimal_point: ",",
    indian_grouping: false,
};
const APOS_DOT: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "'",
    decimal_point: ".",
    indian_grouping: false,
};
const APOS_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "'",
    decimal_point: ",",
    indian_grouping: false,
};
const NONE_DOT: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "",
    decimal_point: ".",
    indian_grouping: false,
};
const INDIAN: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: ",",
    decimal_point: ".",
    indian_grouping: true,
};

const COMMA_DOT_LOCALES: &[&str] = &[
    "aa_et", "af_za", "ak_gh", "am_et", "ar_ae", "ar_bh", "ar_dz", "ar_eg", "ar_in", "ar_iq",
    "ar_jo", "ar_kw", "ar_lb", "ar_ly", "ar_ma", "ar_om", "ar_qa", "ar_sd", "ar_ss", "ar_sy",
    "ar_tn", "ar_ye", "az_ir", "bi_vu", "bo_cn", "bo_in", "cy_gb", "dv_mv", "en_ag", "en_au",
    "en_bw", "en_ca", "en_gb", "en_hk", "en_ie", "en_il", "en_ng", "en_nz", "en_ph", "en_sg",
    "en_us", "en_za", "en_zm", "en_zw", "es_do", "es_gt", "es_hn", "es_ni", "es_pa", "es_pr",
    "es_sv", "es_us", "fa_ir", "ga_ie", "gd_gb", "gu_in", "gv_gb", "ha_ng", "he_il", "hi_in",
    "hy_am", "ig_ng", "ik_ca", "iu_ca", "ja_jp", "km_kh", "kn_in", "ko_kr", "ks_in", "kw_gb",
    "lg_ug", "lo_la", "mi_nz", "mr_in", "ms_my", "mt_mt", "my_mm", "ne_np", "nr_za", "om_et",
    "om_ke", "pa_in", "pa_pk", "sa_in", "sd_in", "si_lk", "sm_ws", "so_et", "so_ke", "so_so",
    "ss_za", "st_za", "sw_ke", "sw_tz", "th_th", "ti_et", "tk_tm", "tl_ph", "tn_za", "to_to",
    "ts_za", "ug_cn", "ur_in", "ur_pk", "ve_za", "xh_za", "yi_us", "yo_ng", "zh_cn", "zh_hk",
    "zh_sg", "zh_tw", "zu_za", "an_es", "az_az", "ca_ad", "ca_fr", "ca_it", "de_it", "en_dk",
    "es_pe", "ff_sn", "fy_de", "fy_nl", "ka_ge", "kl_gl", "ku_tr", "lb_lu", "li_be", "li_nl",
    "nl_aw", "sc_it", "se_no", "sq_mk", "tg_tj", "tr_cy", "wa_be", "br_fr", "kk_kz", "nn_no",
    "oc_fr", "uz_uz", "bs_ba", "el_cy", "es_cu", "ln_cd", "mg_mg", "rw_rw", "sr_me", "wo_sn",
    "es_mx", "ce_ru", "cv_ru", "ht_ht", "ia_fr", "ky_kg", "os_ru", "tt_ru", "aa_dj", "aa_er",
    "so_dj", "ti_er", "ps_af", "kv_ru", "su_id",
];
const DOT_COMMA_LOCALES: &[&str] = &[
    "be_by", "da_dk", "de_be", "de_de", "de_lu", "es_ar", "es_bo", "es_cl", "es_co", "es_ec",
    "es_es", "es_py", "es_uy", "es_ve", "fo_fo", "hu_hu", "id_id", "is_is", "lt_lt", "mn_mn",
    "ro_ro", "ru_ua", "sq_al", "tr_tr", "vi_vn", "nb_no", "uk_ua", "no_no",
];
const SPACE_COMMA_LOCALES: &[&str] = &[
    "cs_cz", "es_cr", "et_ee", "fi_fi", "lv_lv", "mk_mk", "ru_ru", "sk_sk", "sv_fi", "sv_se",
];
const NONE_COMMA_LOCALES: &[&str] = &[
    "el_gr", "gl_es", "pt_pt", "sl_si", "ca_es", "de_at", "eu_es", "fr_be", "hr_hr", "it_it",
    "nl_be", "nl_nl", "pt_br", "fr_ca", "fr_fr", "fr_lu", "pl_pl", "fr_ch", "bg_bg",
];

/// Returns a locale's rules and whether the case-insensitive locale was found.
/// Unknown locales use the source `en_US` fallback while returning false.
#[must_use]
pub fn locale_format_style(locale: &str) -> (LocaleFormatStyle, bool) {
    let locale = locale.to_lowercase();
    let key = locale.as_str();
    if COMMA_DOT_LOCALES.contains(&key) {
        (COMMA_DOT, true)
    } else if DOT_COMMA_LOCALES.contains(&key) {
        (DOT_COMMA, true)
    } else if SPACE_COMMA_LOCALES.contains(&key) {
        (SPACE_COMMA, true)
    } else if NONE_COMMA_LOCALES.contains(&key) {
        (NONE_COMMA, true)
    } else {
        match key {
            "de_ch" => (APOS_DOT, true),
            "it_ch" => (APOS_COMMA, true),
            "ar_sa" | "sr_rs" => (NONE_DOT, true),
            "en_in" | "ta_in" | "te_in" => (INDIAN, true),
            _ => (COMMA_DOT, false),
        }
    }
}

fn standard_grouping_bytes(integer: &[u8], separator: &[u8]) -> Vec<u8> {
    let first = match integer.len() % 3 {
        0 if !integer.is_empty() => 3,
        value => value,
    };
    let mut result = Vec::with_capacity(integer.len() + integer.len() / 3 * separator.len());
    result.extend_from_slice(&integer[..first]);
    for position in (first..integer.len()).step_by(3) {
        result.extend_from_slice(separator);
        result.extend_from_slice(&integer[position..position + 3]);
    }
    result
}
fn indian_grouping_bytes(integer: &[u8], separator: &[u8]) -> Vec<u8> {
    if integer.len() <= 3 {
        return integer.to_owned();
    }
    let split = integer.len() - 3;
    let remaining = &integer[..split];
    let mut first = remaining.len() % 2;
    if first == 0 && !remaining.is_empty() {
        first = 2
    }
    let mut result = Vec::with_capacity(integer.len() + integer.len() / 2 * separator.len());
    result.extend_from_slice(&remaining[..first]);
    for position in (first..remaining.len()).step_by(2) {
        result.extend_from_slice(separator);
        result.extend_from_slice(&remaining[position..position + 2]);
    }
    result.extend_from_slice(separator);
    result.extend_from_slice(&integer[split..]);
    result
}

fn scalar_grouping(integer: &str, separator: &str, indian: bool) -> String {
    let characters: Vec<_> = integer.chars().collect();
    let mut groups = Vec::new();
    let mut end = characters.len();
    let mut width = 3;
    while end > 0 {
        let start = end.saturating_sub(width);
        groups.push(characters[start..end].iter().collect::<String>());
        end = start;
        if indian {
            width = 2;
        }
    }
    groups.reverse();
    groups.join(separator)
}

/// Input-domain error for the caller-guaranteed non-empty source arguments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmptyLocaleNumberInput;
impl std::fmt::Display for EmptyLocaleNumberInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("number and precision must be non-empty")
    }
}
impl std::error::Error for EmptyLocaleNumberInput {}

/// Formats a decimal string using MySQL locale rules, returning whether the
/// locale was recognized. Numeric conversion intentionally follows the source:
/// it truncates at the first invalid character and pads/truncates precision.
pub fn format_by_locale(
    number: &str,
    precision: &str,
    locale: &str,
) -> Result<(String, bool), EmptyLocaleNumberInput> {
    if number.is_empty() || precision.is_empty() {
        return Err(EmptyLocaleNumberInput);
    }
    let (style, found) = locale_format_style(locale);
    Ok((format_with_style(number, precision, style), found))
}

fn format_with_style(number: &str, precision: &str, style: LocaleFormatStyle) -> String {
    // The Go source applies unicode.IsDigit to rune(precision[0]), which is a
    // single byte converted to a rune. Consequently only an ASCII first digit
    // can start a precision, while subsequent runes use Unicode Nd membership.
    let precision = if precision.as_bytes()[0].is_ascii_digit() {
        &precision[..precision
            .char_indices()
            .find_map(|(i, c)| (!is_unicode_decimal_digit(c)).then_some(i))
            .unwrap_or(precision.len())]
    } else {
        "0"
    };
    let places = precision.parse::<u64>().ok();
    let normalized = if let Some(rest) = number.strip_prefix("-.") {
        format!("-0.{rest}")
    } else if let Some(rest) = number.strip_prefix('.') {
        format!("0.{rest}")
    } else {
        number.to_owned()
    };
    let number = normalized.as_str();
    // Go repeats the byte-to-rune conversion for the first number digit.
    let invalid = if let Some(rest) = number.strip_prefix('-') {
        !rest.as_bytes().first().is_some_and(u8::is_ascii_digit)
    } else {
        !number.as_bytes()[0].is_ascii_digit()
    };
    if invalid {
        let mut result = "0".to_owned();
        if let Some(value) = places.filter(|v| *v > 0) {
            result.push_str(style.decimal_point);
            if let Ok(value) = usize::try_from(value) {
                result.extend(std::iter::repeat_n('0', value));
            }
        }
        return result;
    }
    let (sign, unsigned) = if let Some(rest) = number.strip_prefix('-') {
        ("-", rest)
    } else {
        ("", number)
    };
    let second_is_dot = unsigned.as_bytes().get(1) == Some(&b'.');
    let valid_end = unsigned
        .char_indices()
        .find_map(|(i, c)| {
            if is_unicode_decimal_digit(c) || i == 1 && second_is_dot || c == '.' && !second_is_dot
            {
                None
            } else {
                Some(i)
            }
        })
        .unwrap_or(unsigned.len());
    let valid = &unsigned[..valid_end];
    let parts: Vec<_> = valid.split('.').collect();
    let integer = parts[0];
    let grouped = if style.thousands_separator.is_empty() {
        integer.as_bytes().to_owned()
    } else if style.indian_grouping {
        indian_grouping_bytes(integer.as_bytes(), style.thousands_separator.as_bytes())
    } else {
        standard_grouping_bytes(integer.as_bytes(), style.thousands_separator.as_bytes())
    };
    let mut result = sign.as_bytes().to_vec();
    result.extend_from_slice(&grouped);
    if let Some(value) = places
        .filter(|v| *v > 0)
        .and_then(|v| usize::try_from(v).ok())
    {
        result.extend_from_slice(style.decimal_point.as_bytes());
        let fraction = if parts.len() == 2 { parts[1] } else { "" };
        let take = fraction.len().min(value);
        result.extend_from_slice(&fraction.as_bytes()[..take]);
        result.extend(std::iter::repeat_n(b'0', value - take));
    }
    if let Ok(result) = String::from_utf8(result) {
        return result;
    }

    // Go's grouping and precision count UTF-8 bytes and can therefore return
    // malformed UTF-8 when a separator or precision boundary splits a rune.
    // Rust's public String contract cannot represent those bytes. Only for
    // that otherwise-unrepresentable case, regroup and truncate by Unicode
    // scalar values; valid Go UTF-8 output is always returned byte-for-byte.
    let mut normalized = String::from(sign);
    normalized.push_str(&scalar_grouping(
        integer,
        style.thousands_separator,
        style.indian_grouping,
    ));
    if let Some(value) = places
        .filter(|v| *v > 0)
        .and_then(|v| usize::try_from(v).ok())
    {
        normalized.push_str(style.decimal_point);
        let fraction = if parts.len() == 2 { parts[1] } else { "" };
        let mut fraction = fraction.chars();
        for _ in 0..value {
            normalized.push(fraction.next().unwrap_or('0'));
        }
    }
    normalized
}
