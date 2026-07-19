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

package expression

// CloneWithArgs clones builtin function signatures while replacing their arguments.
// These methods are intentionally kept together because every concrete builtin signature
// has the same extra clone requirement for memoized expression rewrites.

func (s *builtinArithmeticPlusIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticPlusIntSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticPlusDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticPlusDecimalSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticPlusRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticPlusRealSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMinusRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMinusRealSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMinusDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMinusDecimalSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMinusIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMinusIntSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMultiplyRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMultiplyRealSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMultiplyDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMultiplyDecimalSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMultiplyIntUnsignedSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMultiplyIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMultiplyIntSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticDivideRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticDivideRealSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticDivideDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticDivideDecimalSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticIntDivideIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticIntDivideIntSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticIntDivideDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticIntDivideDecimalSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticModRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticModRealSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticModDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticModDecimalSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticModIntUnsignedUnsignedSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticModIntUnsignedUnsignedSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticModIntUnsignedSignedSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticModIntUnsignedSignedSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticModIntSignedUnsignedSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticModIntSignedUnsignedSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticModIntSignedSignedSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticModIntSignedSignedSig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticPlusVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticPlusVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMinusVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMinusVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (s *builtinArithmeticMultiplyVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinArithmeticMultiplyVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&s.baseBuiltinFunc, args)
	return newSig
}

func (b *castJSONAsArrayFunctionSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &castJSONAsArrayFunctionSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastUnsupportedAsVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastUnsupportedAsVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastVectorFloat32AsUnsupportedSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastVectorFloat32AsUnsupportedSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastStringAsVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastVectorFloat32AsVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastVectorFloat32AsVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastIntAsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastIntAsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastIntAsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastIntAsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastIntAsDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastIntAsDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastIntAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastIntAsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastIntAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastIntAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastIntAsDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastIntAsDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastIntAsJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastIntAsJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastRealAsJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastRealAsJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDecimalAsJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDecimalAsJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastStringAsJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDurationAsJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDurationAsJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastTimeAsJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastTimeAsJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastRealAsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastRealAsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastRealAsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastRealAsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastRealAsDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastRealAsDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastRealAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastRealAsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastRealAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastRealAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastRealAsDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastRealAsDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDecimalAsDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDecimalAsDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastDecimalAsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDecimalAsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastDecimalAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDecimalAsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDecimalAsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDecimalAsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastDecimalAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDecimalAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDecimalAsDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDecimalAsDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastStringAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastStringAsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastStringAsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastStringAsDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastStringAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastStringAsDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastStringAsDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastTimeAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastTimeAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastTimeAsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastTimeAsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastTimeAsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastTimeAsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastTimeAsDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastTimeAsDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastTimeAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastTimeAsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastTimeAsDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastTimeAsDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDurationAsDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDurationAsDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDurationAsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDurationAsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastDurationAsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDurationAsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastDurationAsDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDurationAsDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastDurationAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDurationAsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastDurationAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastDurationAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastJSONAsJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastJSONAsJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastJSONAsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastJSONAsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastJSONAsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastJSONAsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastJSONAsDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastJSONAsDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinCastFunc, args)
	return newSig
}

func (b *builtinCastJSONAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastJSONAsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastVectorFloat32AsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastVectorFloat32AsStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastJSONAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastJSONAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCastJSONAsDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCastJSONAsDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCoalesceVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoalesceVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGreatestIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGreatestRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGreatestDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGreatestStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGreatestCmpStringAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestCmpStringAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinGreatestTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinGreatestDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGreatestVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGreatestVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeastIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeastRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeastDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeastStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeastCmpStringAsTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastCmpStringAsTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinLeastTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.cmpAsDate = b.cmpAsDate
	return newSig
}

func (b *builtinLeastDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeastVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeastVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIntervalIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIntervalIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.hasNullable = b.hasNullable
	return newSig
}

func (b *builtinIntervalRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIntervalRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.hasNullable = b.hasNullable
	return newSig
}

func (b *builtinLTIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLEIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLEIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLERealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLERealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLEDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLEDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLEStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLEStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLEDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLEDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLETimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLETimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLEJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLEJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLEVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLEVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGTVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGTVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGEIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGEIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGERealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGERealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGEDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGEDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGEStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGEStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGEDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGEDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGETimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGETimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGEJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGEJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGEVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGEVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEQVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEQVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNEIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNEIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNERealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNERealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNEDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNEDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNEStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNEStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNEDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNEDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNETimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNETimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNEJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNEJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNEVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNEVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullEQVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullEQVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCaseWhenVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCaseWhenVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIfNullVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIfNullVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInternalToBinarySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInternalToBinarySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInternalFromBinarySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInternalFromBinarySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.cannotConvertStringAsWarning = b.cannotConvertStringAsWarning
	return newSig
}

func (b *builtinAesDecryptSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAesDecryptSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

func (b *builtinAesDecryptIVSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAesDecryptIVSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

func (b *builtinAesEncryptSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAesEncryptSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

func (b *builtinAesEncryptIVSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAesEncryptIVSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

func (b *builtinDecodeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDecodeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEncodeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEncodeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinPasswordSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinPasswordSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRandomBytesSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRandomBytesSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMD5Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMD5Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSHA1Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSHA1Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSHA2Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSHA2Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSM3Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSM3Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCompressSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCompressSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUncompressSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUncompressSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUncompressedLengthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUncompressedLengthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValidatePasswordStrengthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValidatePasswordStrengthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

// CloneWithArgs clones BuiltinGroupingImplSig with the specified arguments.
func (b *BuiltinGroupingImplSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &BuiltinGroupingImplSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.mode = b.mode
	newSig.groupingMarks = b.groupingMarks
	// mpp task generation will clone whole plan tree, including every expression related.
	// if grouping function missed cloning this field, the ToPB check will errors.
	newSig.isMetaInited = b.isMetaInited
	return newSig
}

func (b *builtinIlikeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIlikeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDatabaseSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDatabaseSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFoundRowsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFoundRowsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCurrentUserSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCurrentUserSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCurrentRoleSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCurrentRoleSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCurrentResourceGroupSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCurrentResourceGroupSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUserSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUserSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinConnectionIDSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinConnectionIDSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLastInsertIDSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLastInsertIDSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLastInsertIDWithIDSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLastInsertIDWithIDSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVersionSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVersionSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBVersionSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBVersionSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBIsDDLOwnerSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBIsDDLOwnerSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBenchmarkSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBenchmarkSig{constLoopCount: b.constLoopCount}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCharsetSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCharsetSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (c *builtinCoercibilitySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCoercibilitySig{}
	newSig.cloneFromWithArgs(&c.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCollationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCollationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRowCountSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRowCountSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBMVCCInfoSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBMVCCInfoSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBEncodeRecordKeySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBEncodeRecordKeySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBEncodeIndexKeySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBEncodeIndexKeySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBDecodeKeySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBDecodeKeySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBDecodeSQLDigestsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBDecodeSQLDigestsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBEncodeSQLDigestSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBEncodeSQLDigestSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBDecodePlanSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBDecodePlanSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBDecodeBinaryPlanSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBDecodeBinaryPlanSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNextValSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNextValSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLastValSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLastValSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSetValSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSetValSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFormatBytesSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFormatBytesSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFormatNanoTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFormatNanoTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONTypeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONTypeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONExtractSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONExtractSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONUnquoteSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONUnquoteSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONSetSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONSetSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONInsertSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONInsertSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONReplaceSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONReplaceSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONRemoveSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONRemoveSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONMergeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONMergeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONObjectSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONObjectSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONArraySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONArraySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONContainsPathSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONContainsPathSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONMemberOfSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONMemberOfSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONContainsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONContainsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONOverlapsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONOverlapsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONValidJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONValidJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONValidStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONValidStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONValidOthersSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONValidOthersSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONArrayAppendSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONArrayAppendSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONArrayInsertSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONArrayInsertSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONMergePatchSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONMergePatchSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONSPrettySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONSPrettySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONQuoteSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONQuoteSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONSearchSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONSearchSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONStorageFreeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONStorageFreeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONStorageSizeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONStorageSizeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONDepthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONDepthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONKeysSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONKeysSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONKeys2ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONKeys2ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONLengthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONLengthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONSchemaValidSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONSchemaValidSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLikeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLikeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAbsRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAbsRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAbsIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAbsIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAbsUIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAbsUIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAbsDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAbsDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRoundRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRoundRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRoundIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRoundIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRoundDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRoundDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRoundWithFracRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRoundWithFracRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRoundWithFracIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRoundWithFracIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRoundWithFracDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRoundWithFracDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCeilRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCeilRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCeilIntToIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCeilIntToIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCeilIntToDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCeilIntToDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCeilDecToIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCeilDecToIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCeilDecToDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCeilDecToDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFloorRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFloorRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFloorIntToIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFloorIntToIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFloorIntToDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFloorIntToDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFloorDecToIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFloorDecToIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFloorDecToDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFloorDecToDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLog1ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLog1ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLog2ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLog2ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLog2Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLog2Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLog10Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLog10Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRandSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRandSig{mysqlRng: b.mysqlRng}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRandWithSeedFirstGenSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRandWithSeedFirstGenSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinPowSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinPowSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinConvSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinConvSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCRC32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCRC32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSignSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSignSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSqrtSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSqrtSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAcosSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAcosSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAsinSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAsinSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAtan1ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAtan1ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAtan2ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAtan2ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCosSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCosSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCotSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCotSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDegreesSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDegreesSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinExpSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinExpSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinPISig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinPISig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRadiansSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRadiansSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSinSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSinSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTanSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTanSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTruncateDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTruncateDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTruncateRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTruncateRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTruncateIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTruncateIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTruncateUintSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTruncateUintSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSleepSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSleepSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLockSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLockSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinReleaseLockSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinReleaseLockSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDecimalAnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDecimalAnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDurationAnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDurationAnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIntAnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIntAnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinJSONAnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinJSONAnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVectorFloat32AnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVectorFloat32AnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRealAnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRealAnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStringAnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStringAnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeAnyValueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeAnyValueSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInetAtonSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInetAtonSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInetNtoaSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInetNtoaSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInet6AtonSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInet6AtonSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInet6NtoaSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInet6NtoaSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFreeLockSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFreeLockSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIsIPv4Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIsIPv4Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIsIPv4CompatSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIsIPv4CompatSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIsIPv4MappedSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIsIPv4MappedSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIsIPv6Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIsIPv6Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUsedLockSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUsedLockSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIsUUIDSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIsUUIDSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNameConstTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNameConstTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinReleaseAllLocksSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinReleaseAllLocksSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUUIDSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUUIDSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVitessHashSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVitessHashSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUUIDToBinSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUUIDToBinSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBinToUUIDSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBinToUUIDSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTidbShardSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTidbShardSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLogicAndSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLogicAndSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLogicOrSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLogicOrSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLogicXorSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLogicXorSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBitAndSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBitAndSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBitOrSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBitOrSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBitXorSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBitXorSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeftShiftSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeftShiftSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRightShiftSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRightShiftSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRealIsTrueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRealIsTrueSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDecimalIsTrueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDecimalIsTrueSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIntIsTrueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIntIsTrueSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVectorFloat32IsTrueSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVectorFloat32IsTrueSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRealIsFalseSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRealIsFalseSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDecimalIsFalseSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDecimalIsFalseSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIntIsFalseSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIntIsFalseSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVectorFloat32IsFalseSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVectorFloat32IsFalseSig{keepNull: b.keepNull}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBitNegSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBitNegSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnaryNotRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnaryNotRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnaryNotDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnaryNotDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnaryNotIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnaryNotIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnaryNotJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnaryNotJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnaryMinusIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnaryMinusIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnaryMinusDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnaryMinusDecimalSig{constantArgOverflow: b.constantArgOverflow}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnaryMinusRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnaryMinusRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDecimalIsNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDecimalIsNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDurationIsNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDurationIsNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinIntIsNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinIntIsNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRealIsNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRealIsNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStringIsNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStringIsNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVectorFloat32IsNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVectorFloat32IsNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeIsNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeIsNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInDecimalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInJSONSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInVectorFloat32Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRowSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRowSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSetStringVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSetStringVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSetRealVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSetRealVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSetDecimalVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSetDecimalVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSetIntVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSetIntVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSetTimeVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSetTimeVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGetStringVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGetStringVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGetIntVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGetIntVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGetRealVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGetRealVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGetDecimalVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGetDecimalVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGetTimeVarSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGetTimeVarSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesIntSig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesRealSig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesDecimalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesDecimalSig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesStringSig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesTimeSig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesDurationSig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesJSONSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesJSONSig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinValuesVectorFloat32Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinValuesVectorFloat32Sig{offset: b.offset}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBitCountSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBitCountSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGetParamStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGetParamStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (re *builtinRegexpLikeFuncSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRegexpLikeFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.cloneWithArgs(args)
	return newSig
}

func (re *builtinRegexpSubstrFuncSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRegexpSubstrFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.cloneWithArgs(args)
	return newSig
}

func (re *builtinRegexpInStrFuncSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRegexpInStrFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.cloneWithArgs(args)
	return newSig
}

func (re *builtinRegexpReplaceFuncSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRegexpReplaceFuncSig{}
	newSig.regexpBaseFuncSig = *re.regexpBaseFuncSig.cloneWithArgs(args)
	return newSig
}

func (b *builtinLengthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLengthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinASCIISig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinASCIISig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinConcatSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinConcatSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinConcatWSSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinConcatWSSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinLeftSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeftSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLeftUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLeftUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRightSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRightSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRightUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRightUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRepeatSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRepeatSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinLowerUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLowerUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLowerSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLowerSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinReverseSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinReverseSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinReverseUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinReverseUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSpaceSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSpaceSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinUpperUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUpperUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUpperSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUpperSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStrcmpSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStrcmpSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinReplaceSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinReplaceSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinConvertSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinConvertSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubstring2ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubstring2ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubstring2ArgsUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubstring2ArgsUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubstring3ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubstring3ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubstring3ArgsUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubstring3ArgsUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubstringIndexSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubstringIndexSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLocate2ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLocate2ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLocate2ArgsUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLocate2ArgsUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLocate3ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLocate3ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLocate3ArgsUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLocate3ArgsUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinHexStrArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinHexStrArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinHexIntArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinHexIntArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnHexSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnHexSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTrim1ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTrim1ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTrim2ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTrim2ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTrim3ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTrim3ArgsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLTrimSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLTrimSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinRTrimSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRTrimSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLpadSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLpadSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinLpadUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLpadUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinRpadSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRpadSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinRpadUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinRpadUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinBitLengthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBitLengthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCharSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCharSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCharLengthBinarySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCharLengthBinarySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCharLengthUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCharLengthUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFindInSetSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFindInSetSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFieldIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFieldIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFieldRealSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFieldRealSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFieldStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFieldStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMakeSetSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMakeSetSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinOctIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinOctIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinOctStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinOctStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinOrdSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinOrdSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinQuoteSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinQuoteSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinBinSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinBinSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinEltSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinEltSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinExportSet3ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinExportSet3ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinExportSet4ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinExportSet4ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinExportSet5ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinExportSet5ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFormatWithLocaleSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFormatWithLocaleSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFormatSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFormatSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFromBase64Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFromBase64Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinToBase64Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinToBase64Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinInsertSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInsertSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinInsertUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInsertUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinInstrUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInstrUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinInstrSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinInstrSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLoadFileSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLoadFileSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinWeightStringNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinWeightStringNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinWeightStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinWeightStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.padding = b.padding
	newSig.length = b.length
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

func (b *builtinTranslateBinarySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTranslateBinarySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTranslateUTF8Sig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTranslateUTF8Sig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDateSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDateSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDateLiteralSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDateLiteralSig{literal: b.literal}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDateDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDateDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDurationDurationTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDurationDurationTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeTimeTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeTimeTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDurationStringTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDurationStringTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStringDurationTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStringDurationTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeStringTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeStringTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStringTimeTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStringTimeTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStringStringTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStringStringTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNullTimeDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNullTimeDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDateFormatSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDateFormatSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFromDaysSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFromDaysSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinHourSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinHourSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMinuteSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMinuteSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSecondSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSecondSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMicroSecondSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMicroSecondSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMonthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMonthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMonthNameSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMonthNameSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDayNameSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDayNameSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDayOfMonthSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDayOfMonthSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDayOfWeekSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDayOfWeekSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinDayOfYearSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinDayOfYearSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinWeekWithModeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinWeekWithModeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinWeekWithoutModeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinWeekWithoutModeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinWeekDaySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinWeekDaySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinWeekOfYearSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinWeekOfYearSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinYearSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinYearSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinYearWeekWithModeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinYearWeekWithModeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinYearWeekWithoutModeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinYearWeekWithoutModeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFromUnixTime1ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFromUnixTime1ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinFromUnixTime2ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinFromUnixTime2ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinGetFormatSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinGetFormatSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStrToDateDateSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStrToDateDateSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStrToDateDatetimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStrToDateDatetimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinStrToDateDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinStrToDateDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSysDateWithFspSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSysDateWithFspSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSysDateWithoutFspSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSysDateWithoutFspSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCurrentDateSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCurrentDateSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCurrentTime0ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCurrentTime0ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinCurrentTime1ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinCurrentTime1ArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeLiteralSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeLiteralSig{duration: b.duration}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUTCDateSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUTCDateSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUTCTimestampWithArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUTCTimestampWithArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUTCTimestampWithoutArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUTCTimestampWithoutArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNowWithArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNowWithArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinNowWithoutArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinNowWithoutArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinExtractDatetimeFromStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinExtractDatetimeFromStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinExtractDatetimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinExtractDatetimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinExtractDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinExtractDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddSubDateAsStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddSubDateAsStringSig{
		baseDateArithmetical: b.baseDateArithmetical,
		getDate:              b.getDate,
		vecGetDate:           b.vecGetDate,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
	}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddSubDateDatetimeAnySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddSubDateDatetimeAnySig{
		baseDateArithmetical: b.baseDateArithmetical,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
	}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddSubDateDurationAnySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddSubDateDurationAnySig{
		baseDateArithmetical: b.baseDateArithmetical,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
		durationOp:           b.durationOp,
	}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimestampDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimestampDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnixTimestampCurrentSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnixTimestampCurrentSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnixTimestampIntSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnixTimestampIntSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUnixTimestampDecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUnixTimestampDecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimestamp1ArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimestamp1ArgSig{isFloat: b.isFloat}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimestamp2ArgsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimestamp2ArgsSig{isFloat: b.isFloat}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimestampLiteralSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimestampLiteralSig{tm: b.tm}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddTimeDateTimeNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddTimeDateTimeNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddDatetimeAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddDatetimeAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddDatetimeAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddDatetimeAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddTimeDurationNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddTimeDurationNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddDurationAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddDurationAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddDurationAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddDurationAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddTimeStringNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddTimeStringNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddStringAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddStringAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddStringAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddStringAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddDateAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddDateAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinAddDateAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinAddDateAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinConvertTzSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinConvertTzSig{timezoneRegex: b.timezoneRegex}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMakeDateSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMakeDateSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinMakeTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinMakeTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinPeriodAddSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinPeriodAddSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinPeriodDiffSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinPeriodDiffSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinQuarterSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinQuarterSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSecToTimeSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSecToTimeSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubDatetimeAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubDatetimeAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubDatetimeAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubDatetimeAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubTimeDateTimeNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubTimeDateTimeNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubStringAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubStringAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubStringAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubStringAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubTimeStringNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubTimeStringNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubDurationAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubDurationAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubDurationAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubDurationAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubTimeDurationNullSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubTimeDurationNullSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubDateAndDurationSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubDateAndDurationSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinSubDateAndStringSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinSubDateAndStringSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeFormatSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeFormatSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimeToSecSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimeToSecSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTimestampAddSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTimestampAddSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinToDaysSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinToDaysSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinToSecondsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinToSecondsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUTCTimeWithoutArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUTCTimeWithoutArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinUTCTimeWithArgSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinUTCTimeWithArgSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinLastDaySig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinLastDaySig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTidbParseTsoSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTidbParseTsoSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTidbParseTsoLogicalSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTidbParseTsoLogicalSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBBoundedStalenessSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBBoundedStalenessSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinTiDBCurrentTsoSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinTiDBCurrentTsoSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecDimsSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecDimsSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecL1DistanceSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecL1DistanceSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecL2DistanceSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecL2DistanceSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecNegativeInnerProductSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecNegativeInnerProductSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecCosineDistanceSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecCosineDistanceSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecL2NormSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecL2NormSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecFromTextSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecFromTextSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *builtinVecAsTextSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &builtinVecAsTextSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	return newSig
}

func (b *extensionFuncSig) CloneWithArgs(args []Expression) builtinFunc {
	newSig := &extensionFuncSig{}
	newSig.cloneFromWithArgs(&b.baseBuiltinFunc, args)
	newSig.FunctionDef = b.FunctionDef
	return newSig
}
