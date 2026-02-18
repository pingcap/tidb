// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package expression

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

var (
	_ functionClass = &lengthFunctionClass{}
	_ functionClass = &asciiFunctionClass{}
	_ functionClass = &concatFunctionClass{}
	_ functionClass = &concatWSFunctionClass{}
	_ functionClass = &leftFunctionClass{}
	_ functionClass = &repeatFunctionClass{}
	_ functionClass = &lowerFunctionClass{}
	_ functionClass = &reverseFunctionClass{}
	_ functionClass = &spaceFunctionClass{}
	_ functionClass = &upperFunctionClass{}
	_ functionClass = &strcmpFunctionClass{}
	_ functionClass = &replaceFunctionClass{}
	_ functionClass = &convertFunctionClass{}
	_ functionClass = &substringFunctionClass{}
	_ functionClass = &substringIndexFunctionClass{}
	_ functionClass = &locateFunctionClass{}
	_ functionClass = &hexFunctionClass{}
	_ functionClass = &unhexFunctionClass{}
	_ functionClass = &trimFunctionClass{}
	_ functionClass = &lTrimFunctionClass{}
	_ functionClass = &rTrimFunctionClass{}
	_ functionClass = &lpadFunctionClass{}
	_ functionClass = &rpadFunctionClass{}
	_ functionClass = &bitLengthFunctionClass{}
	_ functionClass = &charFunctionClass{}
	_ functionClass = &charLengthFunctionClass{}
	_ functionClass = &findInSetFunctionClass{}
	_ functionClass = &fieldFunctionClass{}
	_ functionClass = &makeSetFunctionClass{}
	_ functionClass = &octFunctionClass{}
	_ functionClass = &ordFunctionClass{}
	_ functionClass = &quoteFunctionClass{}
	_ functionClass = &binFunctionClass{}
	_ functionClass = &eltFunctionClass{}
	_ functionClass = &exportSetFunctionClass{}
	_ functionClass = &formatFunctionClass{}
	_ functionClass = &fromBase64FunctionClass{}
	_ functionClass = &toBase64FunctionClass{}
	_ functionClass = &insertFunctionClass{}
	_ functionClass = &instrFunctionClass{}
	_ functionClass = &loadFileFunctionClass{}
	_ functionClass = &weightStringFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinASCIISig{}
	_ builtinFunc = &builtinConcatSig{}
	_ builtinFunc = &builtinConcatWSSig{}
	_ builtinFunc = &builtinLeftSig{}
	_ builtinFunc = &builtinLeftUTF8Sig{}
	_ builtinFunc = &builtinRightSig{}
	_ builtinFunc = &builtinRightUTF8Sig{}
	_ builtinFunc = &builtinRepeatSig{}
	_ builtinFunc = &builtinLowerUTF8Sig{}
	_ builtinFunc = &builtinLowerSig{}
	_ builtinFunc = &builtinReverseUTF8Sig{}
	_ builtinFunc = &builtinReverseSig{}
	_ builtinFunc = &builtinSpaceSig{}
	_ builtinFunc = &builtinUpperUTF8Sig{}
	_ builtinFunc = &builtinUpperSig{}
	_ builtinFunc = &builtinStrcmpSig{}
	_ builtinFunc = &builtinReplaceSig{}
	_ builtinFunc = &builtinConvertSig{}
	_ builtinFunc = &builtinSubstring2ArgsSig{}
	_ builtinFunc = &builtinSubstring3ArgsSig{}
	_ builtinFunc = &builtinSubstring2ArgsUTF8Sig{}
	_ builtinFunc = &builtinSubstring3ArgsUTF8Sig{}
	_ builtinFunc = &builtinSubstringIndexSig{}
	_ builtinFunc = &builtinLocate2ArgsUTF8Sig{}
	_ builtinFunc = &builtinLocate3ArgsUTF8Sig{}
	_ builtinFunc = &builtinLocate2ArgsSig{}
	_ builtinFunc = &builtinLocate3ArgsSig{}
	_ builtinFunc = &builtinHexStrArgSig{}
	_ builtinFunc = &builtinHexIntArgSig{}
	_ builtinFunc = &builtinUnHexSig{}
	_ builtinFunc = &builtinTrim1ArgSig{}
	_ builtinFunc = &builtinTrim2ArgsSig{}
	_ builtinFunc = &builtinTrim3ArgsSig{}
	_ builtinFunc = &builtinLTrimSig{}
	_ builtinFunc = &builtinRTrimSig{}
	_ builtinFunc = &builtinLpadUTF8Sig{}
	_ builtinFunc = &builtinLpadSig{}
	_ builtinFunc = &builtinRpadUTF8Sig{}
	_ builtinFunc = &builtinRpadSig{}
	_ builtinFunc = &builtinBitLengthSig{}
	_ builtinFunc = &builtinCharSig{}
	_ builtinFunc = &builtinCharLengthUTF8Sig{}
	_ builtinFunc = &builtinFindInSetSig{}
	_ builtinFunc = &builtinMakeSetSig{}
	_ builtinFunc = &builtinOctIntSig{}
	_ builtinFunc = &builtinOctStringSig{}
	_ builtinFunc = &builtinOrdSig{}
	_ builtinFunc = &builtinQuoteSig{}
	_ builtinFunc = &builtinBinSig{}
	_ builtinFunc = &builtinEltSig{}
	_ builtinFunc = &builtinExportSet3ArgSig{}
	_ builtinFunc = &builtinExportSet4ArgSig{}
	_ builtinFunc = &builtinExportSet5ArgSig{}
	_ builtinFunc = &builtinFormatWithLocaleSig{}
	_ builtinFunc = &builtinFormatSig{}
	_ builtinFunc = &builtinFromBase64Sig{}
	_ builtinFunc = &builtinToBase64Sig{}
	_ builtinFunc = &builtinInsertSig{}
	_ builtinFunc = &builtinInsertUTF8Sig{}
	_ builtinFunc = &builtinInstrUTF8Sig{}
	_ builtinFunc = &builtinInstrSig{}
	_ builtinFunc = &builtinFieldRealSig{}
	_ builtinFunc = &builtinFieldIntSig{}
	_ builtinFunc = &builtinFieldStringSig{}
	_ builtinFunc = &builtinWeightStringSig{}
)

func reverseBytes(origin []byte) []byte {
	for i, length := 0, len(origin); i < length/2; i++ {
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	return origin
}

func reverseRunes(origin []rune) []rune {
	for i, length := 0, len(origin); i < length/2; i++ {
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	return origin
}

// SetBinFlagOrBinStr sets resTp to binary string if argTp is a binary string,
// if not, sets the binary flag of resTp to true if argTp has binary flag.
// We need to check if the tp is enum or set, if so, don't add binary flag directly unless it has binary flag.
func SetBinFlagOrBinStr(argTp *types.FieldType, resTp *types.FieldType) {
	nonEnumOrSet := !(argTp.GetType() == mysql.TypeEnum || argTp.GetType() == mysql.TypeSet)
	if types.IsBinaryStr(argTp) {
		types.SetBinChsClnFlag(resTp)
	} else if mysql.HasBinaryFlag(argTp.GetFlag()) || (!types.IsNonBinaryStr(argTp) && nonEnumOrSet) {
		resTp.AddFlag(mysql.BinaryFlag)
	}
}

// addBinFlag add the binary flag to `tp` if its charset is binary
func addBinFlag(tp *types.FieldType) {
	SetBinFlagOrBinStr(tp, tp)
}

type lengthFunctionClass struct {
	baseFunctionClass
}


