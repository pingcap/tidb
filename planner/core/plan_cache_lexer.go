package core

func isNumber(c byte) bool {
	if c >= '0' && c <= '9' {
		return true
	}
	return false
}

func isString(c byte) bool {
	if c == '\'' || c == '"' {
		return true
	}
	return false
}

func isChar(c byte) bool {
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return true
	}
	return false
}

func isSchema(c byte) bool {
	// Incomplete
	if isNumber(c) || isChar(c) || c == '_' {
		return true
	}
	return false
}

// FastLexer ...
func FastLexer(sql string) (string, []string, bool) {
	sqlText := make([]byte, 0, len(sql))
	var constantParams [][]byte
	var constantParam []byte
	isStringStatus := false
	isNumberStatus := false
	isSchemaNameStatus := false

	for _, c := range []byte(sql) {
		if isNumberStatus {
			if isNumber(c) {
				constantParam = append(constantParam, c)
				continue
			} else {
				numConstantParam := make([]byte, 0, len(constantParam))
				// copy(numConstantParam, constantParam)
				for _, ch := range constantParam {
					numConstantParam = append(numConstantParam, ch)
				}
				constantParam = constantParam[:0]
				constantParams = append(constantParams, numConstantParam)
				sqlText = append(sqlText, '?')
				isNumberStatus = false
			}
		} else if isStringStatus {
			constantParam = append(constantParam, c)
			if isString(c) {
				stringConstantParam := make([]byte, 0, len(constantParam))
				// copy(stringConstantParam, constantParam)
				for _, ch := range constantParam {
					stringConstantParam = append(stringConstantParam, ch)
				}
				constantParam = constantParam[:0]
				constantParams = append(constantParams, stringConstantParam)
				sqlText = append(sqlText, '?')
				isStringStatus = false
			}
			continue
		} else if isSchemaNameStatus {
			if !isSchema(c) {
				isSchemaNameStatus = false
			}
		}
		if isNumber(c) && !isSchemaNameStatus {
			isNumberStatus = true
			constantParam = append(constantParam, c)
		} else if isString(c) {
			isStringStatus = true
			constantParam = append(constantParam, c)
		} else {
			if isChar(c) {
				isSchemaNameStatus = true
			}
			sqlText = append(sqlText, c)
		}
	}
	if isStringStatus || isNumberStatus {
		return "", nil, false
	}
	params := make([]string, 0, len(constantParams))
	for _, param := range constantParams {
		params = append(params, string(param))
	}
	return string(sqlText), params, true
}
