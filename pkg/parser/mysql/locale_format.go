package mysql

import (
	"bytes"
	"strconv"
	"strings"
	"unicode"
)

// LocaleFormatStyle defines the rules for number formatting.
type LocaleFormatStyle struct {
	ThousandsSep string // Thousands separator
	DecimalPoint string // Decimal point
}

// Formatting style IDs (descriptive names)
const (
	styleCommaDot         = "CommaDot"         // 123,456.78 (en_US)
	styleDotComma         = "DotComma"         // 123.456,78 (de_DE)
	styleSpaceComma       = "SpaceComma"       // 123 456,78 (fr_FR)
	styleNoneComma        = "NoneComma"        // 123456,78  (bg_BG)
	styleAposDot          = "AposDot"          // 123'456.78 (de_CH)
	styleSpaceDot         = "SpaceDot"         // 123 456.78  (es_MX)
	styleNarrowSpaceComma = "NarrowSpaceComma" // 123 456,78 (ce_RU)
	styleNoneDot          = "NoneDot"          // 123456.78  (ar_SA)
	styleArabic           = "Arabic"           // 123٬456٫78 (ps_AF)
)

// formatStyleMap maps a style ID to its separator definitions.
var formatStyleMap = map[string]LocaleFormatStyle{
	styleCommaDot:         {ThousandsSep: ",", DecimalPoint: "."},
	styleDotComma:         {ThousandsSep: ".", DecimalPoint: ","},
	styleSpaceComma:       {ThousandsSep: " ", DecimalPoint: ","}, // U+0020 or U+00A0
	styleNoneComma:        {ThousandsSep: "", DecimalPoint: ","},  // No thousands separator
	styleAposDot:          {ThousandsSep: "'", DecimalPoint: "."},
	styleSpaceDot:         {ThousandsSep: " ", DecimalPoint: "."},           // U+0020 or U+00A0
	styleNarrowSpaceComma: {ThousandsSep: "\u202F", DecimalPoint: ","},      // Narrow no-break space
	styleNoneDot:          {ThousandsSep: "", DecimalPoint: "."},            // No thousands separator
	styleArabic:           {ThousandsSep: "\u066C", DecimalPoint: "\u066B"}, // Arabic separators
}

// localeToStyleMap maps locale names (lowercase) to their corresponding format style ID.
var localeToStyleMap = map[string]string{
	// Maps to styleCommaDot (123,456.78)
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

	// Maps to styleDotComma (123.456,78)
	"an_es": styleDotComma, "az_az": styleDotComma, "be_by": styleDotComma, "ca_ad": styleDotComma, "ca_es": styleDotComma, "ca_fr": styleDotComma,
	"ca_it": styleDotComma, "da_dk": styleDotComma, "de_at": styleDotComma, "de_be": styleDotComma, "de_de": styleDotComma, "de_it": styleDotComma,
	"de_lu": styleDotComma, "en_dk": styleDotComma, "es_ar": styleDotComma, "es_bo": styleDotComma, "es_cl": styleDotComma, "es_co": styleDotComma,
	"es_ec": styleDotComma, "es_es": styleDotComma, "es_pe": styleDotComma, "es_py": styleDotComma, "es_uy": styleDotComma, "es_ve": styleDotComma,
	"eu_es": styleDotComma, "ff_sn": styleDotComma, "fo_fo": styleDotComma, "fr_be": styleDotComma, "fy_de": styleDotComma, "fy_nl": styleDotComma,
	"hr_hr": styleDotComma, "hu_hu": styleDotComma, "id_id": styleDotComma, "is_is": styleDotComma, "it_it": styleDotComma, "ka_ge": styleDotComma,
	"kl_gl": styleDotComma, "ku_tr": styleDotComma, "lb_lu": styleDotComma, "li_be": styleDotComma, "li_nl": styleDotComma, "lt_lt": styleDotComma,
	"mn_mn": styleDotComma, "nl_aw": styleDotComma, "nl_be": styleDotComma, "nl_nl": styleDotComma, "pt_br": styleDotComma, "ro_ro": styleDotComma,
	"ru_ua": styleDotComma, "sc_it": styleDotComma, "se_no": styleDotComma, "sq_al": styleDotComma, "sq_mk": styleDotComma, "su_id": styleDotComma,
	"tg_tj": styleDotComma, "tr_cy": styleDotComma, "tr_tr": styleDotComma, "vi_vn": styleDotComma, "wa_be": styleDotComma,

	// Maps to styleSpaceComma (123 456,78)
	"br_fr": styleSpaceComma, "cs_cz": styleSpaceComma, "es_cr": styleSpaceComma, "et_ee": styleSpaceComma, "fi_fi": styleSpaceComma, "fr_ca": styleSpaceComma,
	"fr_fr": styleSpaceComma, "fr_lu": styleSpaceComma, "kk_kz": styleSpaceComma, "lv_lv": styleSpaceComma, "mk_mk": styleSpaceComma, "nb_no": styleSpaceComma,
	"nn_no": styleSpaceComma, "no_no": styleSpaceComma, "oc_fr": styleSpaceComma, "pl_pl": styleSpaceComma, "ru_ru": styleSpaceComma, "sk_sk": styleSpaceComma,
	"sv_fi": styleSpaceComma, "sv_se": styleSpaceComma, "uk_ua": styleSpaceComma, "uz_uz": styleSpaceComma,

	// Maps to styleNoneComma (123456,78)
	"bg_bg": styleNoneComma, "bs_ba": styleNoneComma, "el_cy": styleNoneComma, "el_gr": styleNoneComma, "es_cu": styleNoneComma, "gl_es": styleNoneComma,
	"ln_cd": styleNoneComma, "mg_mg": styleNoneComma, "pt_pt": styleNoneComma, "rw_rw": styleNoneComma, "sl_si": styleNoneComma, "sr_me": styleNoneComma,
	"sr_rs": styleNoneComma, "wo_sn": styleNoneComma,

	// Maps to styleAposDot (123'456.78)
	"de_ch": styleAposDot, "fr_ch": styleAposDot, "it_ch": styleAposDot,

	// Maps to styleSpaceDot (123 456.78)
	"es_mx": styleSpaceDot,

	// Maps to styleNarrowSpaceComma (123 456,78)
	"ce_ru": styleNarrowSpaceComma, "cv_ru": styleNarrowSpaceComma, "ht_ht": styleNarrowSpaceComma, "ia_fr": styleNarrowSpaceComma, "kv_ru": styleNarrowSpaceComma, "ky_kg": styleNarrowSpaceComma,
	"os_ru": styleNarrowSpaceComma, "tt_ru": styleNarrowSpaceComma,

	// Maps to styleNoneDot (123456.78)
	"aa_dj": styleNoneDot, "aa_er": styleNoneDot, "ar_sa": styleNoneDot, "so_dj": styleNoneDot, "ti_er": styleNoneDot,

	// Maps to styleArabic (123٬456٫78)
	"ps_af": styleArabic,
}

// GetLocaleFormatStyle returns the formatting rules for a given locale.
func GetLocaleFormatStyle(locale string) LocaleFormatStyle {
	styleID, ok := localeToStyleMap[strings.ToLower(locale)]
	if !ok {
		// Default to en_US style if locale is not found
		styleID = styleCommaDot
	}
	// It's guaranteed that styleCommaDot exists in formatStyleMap
	return formatStyleMap[styleID]
}

// FormatByLocale is the new exported function to be called by builtin_string.go.
// It replaces the old GetLocaleFormatFunction.
func FormatByLocale(number, precision, locale string) (string, error) {
	// Default to en_US style if locale is NULL
	if locale == "" {
		locale = "en_US"
	}
	style := GetLocaleFormatStyle(locale)
	return formatWithStyle(number, precision, style)
}

// formatWithStyle is the generic formatting function, replacing the old formatENUS
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
	pos := 0

	if len(parts[0])%3 != 0 {
		pos += len(parts[0]) % 3
		buffer.WriteString(parts[0][:pos])
		buffer.WriteString(style.ThousandsSep)
	}
	for ; pos < len(parts[0]); pos += 3 {
		buffer.WriteString(parts[0][pos : pos+3])
		buffer.WriteString(style.ThousandsSep)
	}
	// Truncate the last separator.
	// If len(thousandsSep) is 0, this does nothing.
	buffer.Truncate(buffer.Len() - len(style.ThousandsSep))

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
