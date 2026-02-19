// Copyright 2017 PingCAP, Inc.
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
	_ functionClass = &jsonTypeFunctionClass{}
	_ functionClass = &jsonSumCRC32FunctionClass{}
	_ functionClass = &jsonExtractFunctionClass{}
	_ functionClass = &jsonUnquoteFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSetFunctionClass{}
	_ functionClass = &jsonInsertFunctionClass{}
	_ functionClass = &jsonReplaceFunctionClass{}
	_ functionClass = &jsonRemoveFunctionClass{}
	_ functionClass = &jsonMergeFunctionClass{}
	_ functionClass = &jsonObjectFunctionClass{}
	_ functionClass = &jsonArrayFunctionClass{}
	_ functionClass = &jsonMemberOfFunctionClass{}
	_ functionClass = &jsonContainsFunctionClass{}
	_ functionClass = &jsonOverlapsFunctionClass{}
	_ functionClass = &jsonContainsPathFunctionClass{}
	_ functionClass = &jsonValidFunctionClass{}
	_ functionClass = &jsonArrayAppendFunctionClass{}
	_ functionClass = &jsonArrayInsertFunctionClass{}
	_ functionClass = &jsonMergePatchFunctionClass{}
	_ functionClass = &jsonMergePreserveFunctionClass{}
	_ functionClass = &jsonPrettyFunctionClass{}
	_ functionClass = &jsonQuoteFunctionClass{}
	_ functionClass = &jsonSchemaValidFunctionClass{}
	_ functionClass = &jsonSearchFunctionClass{}
	_ functionClass = &jsonStorageSizeFunctionClass{}
	_ functionClass = &jsonDepthFunctionClass{}
	_ functionClass = &jsonKeysFunctionClass{}
	_ functionClass = &jsonLengthFunctionClass{}

	_ builtinFunc = &builtinJSONTypeSig{}
	_ builtinFunc = &builtinJSONSumCRC32Sig{}
	_ builtinFunc = &builtinJSONQuoteSig{}
	_ builtinFunc = &builtinJSONUnquoteSig{}
	_ builtinFunc = &builtinJSONArraySig{}
	_ builtinFunc = &builtinJSONArrayAppendSig{}
	_ builtinFunc = &builtinJSONArrayInsertSig{}
	_ builtinFunc = &builtinJSONObjectSig{}
	_ builtinFunc = &builtinJSONExtractSig{}
	_ builtinFunc = &builtinJSONSetSig{}
	_ builtinFunc = &builtinJSONInsertSig{}
	_ builtinFunc = &builtinJSONReplaceSig{}
	_ builtinFunc = &builtinJSONRemoveSig{}
	_ builtinFunc = &builtinJSONMergeSig{}
	_ builtinFunc = &builtinJSONMemberOfSig{}
	_ builtinFunc = &builtinJSONContainsSig{}
	_ builtinFunc = &builtinJSONOverlapsSig{}
	_ builtinFunc = &builtinJSONStorageSizeSig{}
	_ builtinFunc = &builtinJSONDepthSig{}
	_ builtinFunc = &builtinJSONSchemaValidSig{}
	_ builtinFunc = &builtinJSONSearchSig{}
	_ builtinFunc = &builtinJSONKeysSig{}
	_ builtinFunc = &builtinJSONKeys2ArgsSig{}
	_ builtinFunc = &builtinJSONLengthSig{}
	_ builtinFunc = &builtinJSONValidJSONSig{}
	_ builtinFunc = &builtinJSONValidStringSig{}
	_ builtinFunc = &builtinJSONValidOthersSig{}
)

