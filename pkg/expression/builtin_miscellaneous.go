// Copyright 2021 PingCAP, Inc.
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

var (
	_ functionClass = &anyValueFunctionClass{}
	_ functionClass = &binToUUIDFunctionClass{}
	_ functionClass = &defaultFunctionClass{}
	_ functionClass = &inet6AtonFunctionClass{}
	_ functionClass = &inet6NtoaFunctionClass{}
	_ functionClass = &inetAtonFunctionClass{}
	_ functionClass = &inetNtoaFunctionClass{}
	_ functionClass = &isFreeLockFunctionClass{}
	_ functionClass = &isIPv4CompatFunctionClass{}
	_ functionClass = &isIPv4FunctionClass{}
	_ functionClass = &isIPv4MappedFunctionClass{}
	_ functionClass = &isIPv6FunctionClass{}
	_ functionClass = &isUsedLockFunctionClass{}
	_ functionClass = &isUUIDFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &nameConstFunctionClass{}
	_ functionClass = &releaseAllLocksFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &sleepFunctionClass{}
	_ functionClass = &tidbRowChecksumFunctionClass{}
	_ functionClass = &tidbShardFunctionClass{}
	_ functionClass = &uuidFunctionClass{}
	_ functionClass = &uuidShortFunctionClass{}
	_ functionClass = &uuidTimestampFunctionClass{}
	_ functionClass = &uuidToBinFunctionClass{}
	_ functionClass = &uuidv4FunctionClass{}
	_ functionClass = &uuidv7FunctionClass{}
	_ functionClass = &uuidVersionFunctionClass{}
	_ functionClass = &vitessHashFunctionClass{}
)

var (
	_ builtinFunc = &builtinBinToUUIDSig{}
	_ builtinFunc = &builtinDecimalAnyValueSig{}
	_ builtinFunc = &builtinDurationAnyValueSig{}
	_ builtinFunc = &builtinFreeLockSig{}
	_ builtinFunc = &builtinInet6AtonSig{}
	_ builtinFunc = &builtinInet6NtoaSig{}
	_ builtinFunc = &builtinInetAtonSig{}
	_ builtinFunc = &builtinInetNtoaSig{}
	_ builtinFunc = &builtinIntAnyValueSig{}
	_ builtinFunc = &builtinIsIPv4CompatSig{}
	_ builtinFunc = &builtinIsIPv4MappedSig{}
	_ builtinFunc = &builtinIsIPv4Sig{}
	_ builtinFunc = &builtinIsIPv6Sig{}
	_ builtinFunc = &builtinIsUUIDSig{}
	_ builtinFunc = &builtinJSONAnyValueSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinNameConstDecimalSig{}
	_ builtinFunc = &builtinNameConstDurationSig{}
	_ builtinFunc = &builtinNameConstIntSig{}
	_ builtinFunc = &builtinNameConstJSONSig{}
	_ builtinFunc = &builtinNameConstRealSig{}
	_ builtinFunc = &builtinNameConstStringSig{}
	_ builtinFunc = &builtinNameConstTimeSig{}
	_ builtinFunc = &builtinNameConstVectorFloat32Sig{}
	_ builtinFunc = &builtinRealAnyValueSig{}
	_ builtinFunc = &builtinReleaseAllLocksSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinStringAnyValueSig{}
	_ builtinFunc = &builtinTidbShardSig{}
	_ builtinFunc = &builtinTimeAnyValueSig{}
	_ builtinFunc = &builtinUsedLockSig{}
	_ builtinFunc = &builtinUUIDSig{}
	_ builtinFunc = &builtinUUIDTimestampSig{}
	_ builtinFunc = &builtinUUIDToBinSig{}
	_ builtinFunc = &builtinUUIDv4Sig{}
	_ builtinFunc = &builtinUUIDv7Sig{}
	_ builtinFunc = &builtinUUIDVersionSig{}
	_ builtinFunc = &builtinVectorFloat32AnyValueSig{}
	_ builtinFunc = &builtinVitessHashSig{}
)

const (
	tidbShardBucketCount = 256
)

