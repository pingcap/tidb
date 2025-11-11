package mysql

import (
	"bytes"
	"strconv"
	"strings"
	"unicode"
)

// LocaleFormatStyle defines the rules for number formatting.
type LocaleFormatStyle struct {
	ThousandsSep     string // Thousands separator
	DecimalPoint     string // Decimal point
	IsIndianGrouping bool   // Special grouping for en_IN, etc. (3,2,2,...)
}

// Formatting style IDs (descriptive names)
const (
	styleCommaDot   = "CommaDot"   // 123,456.78 (en_US)
	styleDotComma   = "DotComma"   // 123.456,78 (de_DE)
	styleSpaceComma = "SpaceComma" // 123 456,78 (fr_FR)
	styleNoneComma  = "NoneComma"  // 123456,78  (bg_BG)
	styleAposDot    = "AposDot"    // 123'456.78 (de_CH)
	styleAposComma  = "AposComma"  // 123'456,78 (it_CH)
	styleNoneDot    = "NoneDot"    // 123456.78  (ar_SA)
	styleIndian     = "Indian"     // 1,23,45,67,890.123 (en_IN)
)

// formatStyleMap maps a style ID to its separator definitions.
var formatStyleMap = map[string]LocaleFormatStyle{
	// IsIndianGrouping is false by default
	styleCommaDot:   {ThousandsSep: ",", DecimalPoint: "."},
	styleDotComma:   {ThousandsSep: ".", DecimalPoint: ","},
	styleSpaceComma: {ThousandsSep: " ", DecimalPoint: ","},
	styleNoneComma:  {ThousandsSep: "", DecimalPoint: ","},
	styleAposDot:    {ThousandsSep: "'", DecimalPoint: "."},
	styleAposComma:  {ThousandsSep: "'", DecimalPoint: ","},
	styleNoneDot:    {ThousandsSep: "", DecimalPoint: "."},
	styleIndian:     {ThousandsSep: ",", DecimalPoint: ".", IsIndianGrouping: true},
}

// localeToStyleMap maps locale names (lowercase) to their corresponding format style ID.
var localeToStyleMap = map[string]string{
	// styleCommaDot (123,456.78): Default/fallback format (e.g., en_US) or where MySQL behavior matches.
	"aa_et": styleCommaDot, "af_za": styleCommaDot, "ak_gh": styleCommaDot, "am_et": styleCommaDot, "ar_ae": styleCommaDot, "ar_bh": styleCommaDot,
	"ar_dz": styleCommaDot, "ar_eg": styleCommaDot, "ar_in": styleCommaDot, "ar_iq": styleCommaDot, "ar_jo": styleCommaDot, "ar_kw": styleCommaDot,
	"ar_lb": styleCommaDot, "ar_ly": styleCommaDot, "ar_ma": styleCommaDot, "ar_om": styleCommaDot, "ar_qa": styleCommaDot, "ar_sd": styleCommaDot,
	"ar_ss": styleCommaDot, "ar_sy": styleCommaDot, "ar_tn": styleCommaDot, "ar_ye": styleCommaDot, "az_ir": styleCommaDot, "bi_vu": styleCommaDot,
	"bo_cn": styleCommaDot, "bo_in": styleCommaDot, "cy_gb": styleCommaDot, "dv_mv": styleCommaDot, "en_ag": styleCommaDot, "en_au": styleCommaDot,
	"en_bw": styleCommaDot, "en_ca": styleCommaDot, "en_gb": styleCommaDot, "en_hk": styleCommaDot, "en_ie": styleCommaDot, "en_il": styleCommaDot,
	"en_ng": styleCommaDot, "en_nz": styleCommaDot, "en_ph": styleCommaDot, "en_sg": styleCommaDot, "en_us": styleCommaDot, "en_za": styleCommaDot,
	"en_zm": styleCommaDot, "en_zw": styleCommaDot, "es_do": styleCommaDot, "es_gt": styleCommaDot, "es_hn": styleCommaDot, "es_ni": styleCommaDot,
	"es_pa": styleCommaDot, "es_pr": styleCommaDot, "es_sv": styleCommaDot, "es_us": styleCommaDot, "fa_ir": styleCommaDot, "ga_ie": styleCommaDot,
	"gd_gb": styleCommaDot, "gu_in": styleCommaDot, "gv_gb": styleCommaDot, "ha_ng": styleCommaDot, "he_il": styleCommaDot, "hi_in": styleCommaDot,
	"hy_am": styleCommaDot, "ig_ng": styleCommaDot, "ik_ca": styleCommaDot, "iu_ca": styleCommaDot, "ja_jp": styleCommaDot, "km_kh": styleCommaDot,
	"kn_in": styleCommaDot, "ko_kr": styleCommaDot, "ks_in": styleCommaDot, "kw_gb": styleCommaDot, "lg_ug": styleCommaDot, "lo_la": styleCommaDot,
	"mi_nz": styleCommaDot, "mr_in": styleCommaDot, "ms_my": styleCommaDot, "mt_mt": styleCommaDot, "my_mm": styleCommaDot, "ne_np": styleCommaDot,
	"nr_za": styleCommaDot, "om_et": styleCommaDot, "om_ke": styleCommaDot, "pa_in": styleCommaDot, "pa_pk": styleCommaDot, "sa_in": styleCommaDot,
	"sd_in": styleCommaDot, "si_lk": styleCommaDot, "sm_ws": styleCommaDot, "so_et": styleCommaDot, "so_ke": styleCommaDot, "so_so": styleCommaDot,
	"ss_za": styleCommaDot, "st_za": styleCommaDot, "sw_ke": styleCommaDot, "sw_tz": styleCommaDot, "th_th": styleCommaDot, "ti_et": styleCommaDot,
	"tk_tm": styleCommaDot, "tl_ph": styleCommaDot, "tn_za": styleCommaDot, "to_to": styleCommaDot, "ts_za": styleCommaDot, "ug_cn": styleCommaDot,
	"ur_in": styleCommaDot, "ur_pk": styleCommaDot, "ve_za": styleCommaDot, "xh_za": styleCommaDot, "yi_us": styleCommaDot, "yo_ng": styleCommaDot,
	"zh_cn": styleCommaDot, "zh_hk": styleCommaDot, "zh_sg": styleCommaDot, "zh_tw": styleCommaDot, "zu_za": styleCommaDot,
	"an_es": styleCommaDot, "az_az": styleCommaDot, "ca_ad": styleCommaDot, "ca_fr": styleCommaDot, "ca_it": styleCommaDot, "de_it": styleCommaDot,
	"en_dk": styleCommaDot, "es_pe": styleCommaDot, "ff_sn": styleCommaDot, "fy_de": styleCommaDot, "fy_nl": styleCommaDot, "ka_ge": styleCommaDot,
	"kl_gl": styleCommaDot, "ku_tr": styleCommaDot, "lb_lu": styleCommaDot, "li_be": styleCommaDot, "li_nl": styleCommaDot, "nl_aw": styleCommaDot,
	"sc_it": styleCommaDot, "se_no": styleCommaDot, "sq_mk": styleCommaDot, "tg_tj": styleCommaDot, "tr_cy": styleCommaDot, "wa_be": styleCommaDot,
	"br_fr": styleCommaDot, "kk_kz": styleCommaDot, "nn_no": styleCommaDot, "oc_fr": styleCommaDot, "uz_uz": styleCommaDot,
	"bs_ba": styleCommaDot, "el_cy": styleCommaDot, "es_cu": styleCommaDot, "ln_cd": styleCommaDot, "mg_mg": styleCommaDot, "rw_rw": styleCommaDot, "sr_me": styleCommaDot, "wo_sn": styleCommaDot,
	"es_mx": styleCommaDot,
	"ce_ru": styleCommaDot, "cv_ru": styleCommaDot, "ht_ht": styleCommaDot, "ia_fr": styleCommaDot, "ky_kg": styleCommaDot, "os_ru": styleCommaDot, "tt_ru": styleCommaDot,
	"aa_dj": styleCommaDot, "aa_er": styleCommaDot, "so_dj": styleCommaDot, "ti_er": styleCommaDot,
	"ps_af": styleCommaDot,
	"kv_ru": styleCommaDot,
	"su_id": styleCommaDot,

	// styleDotComma (123.456,78): Common in Europe and South America.
	"be_by": styleDotComma, "da_dk": styleDotComma, "de_be": styleDotComma, "de_de": styleDotComma, "de_lu": styleDotComma,
	"es_ar": styleDotComma, "es_bo": styleDotComma, "es_cl": styleDotComma, "es_co": styleDotComma,
	"es_ec": styleDotComma, "es_es": styleDotComma, "es_py": styleDotComma, "es_uy": styleDotComma, "es_ve": styleDotComma,
	"fo_fo": styleDotComma, "hu_hu": styleDotComma, "id_id": styleDotComma, "is_is": styleDotComma,
	"lt_lt": styleDotComma, "mn_mn": styleDotComma, "ro_ro": styleDotComma, "ru_ua": styleDotComma, "sq_al": styleDotComma,
	"tr_tr": styleDotComma, "vi_vn": styleDotComma,
	"nb_no": styleDotComma, "uk_ua": styleDotComma,
	"no_no": styleDotComma,

	// styleSpaceComma (123 456,78): Uses space as thousands separator.
	"cs_cz": styleSpaceComma, "es_cr": styleSpaceComma, "et_ee": styleSpaceComma, "fi_fi": styleSpaceComma,
	"lv_lv": styleSpaceComma, "mk_mk": styleSpaceComma,
	"ru_ru": styleSpaceComma, "sk_sk": styleSpaceComma, "sv_fi": styleSpaceComma, "sv_se": styleSpaceComma,

	// styleNoneComma (123456,78): No thousands separator.
	"el_gr": styleNoneComma, "gl_es": styleNoneComma, "pt_pt": styleNoneComma, "sl_si": styleNoneComma,
	"ca_es": styleNoneComma, "de_at": styleNoneComma, "eu_es": styleNoneComma, "fr_be": styleNoneComma, "hr_hr": styleNoneComma, "it_it": styleNoneComma, "nl_be": styleNoneComma, "nl_nl": styleNoneComma, "pt_br": styleNoneComma,
	"fr_ca": styleNoneComma, "fr_fr": styleNoneComma, "fr_lu": styleNoneComma, "pl_pl": styleNoneComma,
	"fr_ch": styleNoneComma,
	"bg_bg": styleNoneComma,

	// styleAposDot (123'456.78): Uses apostrophe as thousands separator.
	"de_ch": styleAposDot,

	// styleAposComma (123'456,78): Uses apostrophe separator, comma decimal.
	"it_ch": styleAposComma,

	// styleNoneDot (123456.78): No thousands separator, dot decimal.
	"ar_sa": styleNoneDot,
	"sr_rs": styleNoneDot,

	// styleIndian (1,23,45,67,890.123): Special Indian grouping (3,2,2,...).
	"en_in": styleIndian,
	"ta_in": styleIndian,
	"te_in": styleIndian,
}

// GetLocaleFormatStyle returns the formatting rules and a bool indicating if the locale was found.
func GetLocaleFormatStyle(locale string) (LocaleFormatStyle, bool) {
	styleID, ok := localeToStyleMap[strings.ToLower(locale)]
	if !ok {
		// Not found. Return default style, but also return 'false'.
		return formatStyleMap[styleCommaDot], false
	}
	// Found. Return style and 'true'.
	return formatStyleMap[styleID], true
}

// FormatByLocale  returns (string, bool, error)
// The bool (found) is true if the locale was found in the map, false otherwise.
func FormatByLocale(number, precision, locale string) (string, bool, error) {
	// 'locale' is guaranteed to be non-empty by the caller (builtin_string.go).
	style, found := GetLocaleFormatStyle(locale)
	// if not found, style is set to default (en_US)
	formattedString, err := formatWithStyle(number, precision, style)
	return formattedString, found, err
}

// formatWithStandardGrouping applies standard 3-digit grouping (e.g., 1,234,567)
func formatWithStandardGrouping(integerPart string, thousandsSep string) string {
	var buffer bytes.Buffer
	partLen := len(integerPart)
	// Find position of first separator
	pos := partLen % 3
	if pos == 0 && partLen > 0 {
		pos = 3
	}

	// Write the first group (1-3 digits)
	buffer.WriteString(integerPart[:pos])

	// Write subsequent 3-digit groups
	for ; pos < partLen; pos += 3 {
		buffer.WriteString(thousandsSep)
		buffer.WriteString(integerPart[pos : pos+3])
	}
	return buffer.String()
}

// formatWithIndianGrouping applies Indian grouping (e.g., 1,23,45,67,890)
func formatWithIndianGrouping(integerPart string, thousandsSep string) string {
	var buffer bytes.Buffer
	s := integerPart
	l := len(s)
	if l <= 3 {
		return s // No grouping needed
	}

	// Get the rightmost 3 digits (e.g., 890)
	rightmost3 := s[l-3:]
	// Get the remaining digits on the left (e.g., 1234567)
	remaining := s[:l-3]
	remLen := len(remaining)

	// Get the first part (1 or 2 digits) (e.g., 1)
	firstPartLen := remLen % 2
	if firstPartLen == 0 && remLen > 0 {
		firstPartLen = 2
	}

	if firstPartLen > 0 {
		buffer.WriteString(remaining[:firstPartLen])
	}

	// Loop through the remaining 2-digit groups (e.g., 23, 45, 67)
	for pos := firstPartLen; pos < remLen; pos += 2 {
		buffer.WriteString(thousandsSep)
		buffer.WriteString(remaining[pos : pos+2])
	}

	// Add the last separator and the rightmost 3 digits
	buffer.WriteString(thousandsSep)
	buffer.WriteString(rightmost3)

	return buffer.String()
}

// formatWithStyle is the generic formatting function.
func formatWithStyle(number string, precision string, style LocaleFormatStyle) (string, error) {
	var buffer bytes.Buffer
	if unicode.IsDigit(rune(precision[0])) {
		for i, v := range precision {
			if unicode.IsDigit(v) {
				continue
			}
			precision = precision[:i]
			break
		}
	} else {
		precision = "0"
	}
	if number[0] == '-' && number[1] == '.' {
		number = strings.Replace(number, "-", "-0", 1)
	} else if number[0] == '.' {
		number = strings.Replace(number, ".", "0.", 1)
	}

	if (number[:1] == "-" && !unicode.IsDigit(rune(number[1]))) ||
		(!unicode.IsDigit(rune(number[0])) && number[:1] != "-") {
		buffer.WriteString("0")
		position, err := strconv.ParseUint(precision, 10, 64)
		if err == nil && position > 0 {
			// Use style-defined decimal point
			buffer.WriteString(style.DecimalPoint)
			buffer.WriteString(strings.Repeat("0", int(position)))
		}
		return buffer.String(), nil
	} else if number[:1] == "-" {
		buffer.WriteString("-")
		number = number[1:]
	}

	for i, v := range number {
		if unicode.IsDigit(v) {
			continue
		} else if i == 1 && number[1] == '.' {
			continue
		} else if v == '.' && number[1] != '.' {
			continue
		}
		number = number[:i]
		break
	}

	parts := strings.Split(number, ".")
	integerPart := parts[0]
	var formattedIntegerPart string

	// Apply grouping logic based on the locale style.
	if len(style.ThousandsSep) == 0 {
		// No separator (e.g., styleNoneComma), just use the integer part
		formattedIntegerPart = integerPart
	} else if style.IsIndianGrouping {
		// Use 3,2,2... grouping for Indian locales.
		formattedIntegerPart = formatWithIndianGrouping(integerPart, style.ThousandsSep)
	} else {
		// Use standard 3-digit grouping.
		formattedIntegerPart = formatWithStandardGrouping(integerPart, style.ThousandsSep)
	}
	buffer.WriteString(formattedIntegerPart)

	position, err := strconv.ParseUint(precision, 10, 64)
	if err == nil {
		if position > 0 {
			buffer.WriteString(style.DecimalPoint) // Use style-defined decimal point
			if len(parts) == 2 {
				if uint64(len(parts[1])) >= position {
					buffer.WriteString(parts[1][:position])
				} else {
					buffer.WriteString(parts[1])
					buffer.WriteString(strings.Repeat("0", int(position)-len(parts[1])))
				}
			} else {
				buffer.WriteString(strings.Repeat("0", int(position)))
			}
		}
	}

	return buffer.String(), nil
}
