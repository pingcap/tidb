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

#include <ctype.h>
#include <stdbool.h>
#include <string.h>

#include "udf_registration_types.h"

extern "C" {

bool word_count_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  (void)initid;
  if (args == nullptr || args->arg_count != 1) {
    if (message != nullptr) {
      const char *msg = "word_count() requires exactly one argument";
      strncpy(message, msg, 255);
      message[255] = 0;
    }
    return true;
  }
  if (args->arg_type != nullptr) {
    args->arg_type[0] = STRING_RESULT;
  }
  return false;
}

void word_count_deinit(UDF_INIT *initid) { (void)initid; }

long long word_count(UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null,
                     unsigned char *error) {
  (void)initid;
  if (is_null != nullptr) {
    *is_null = 0;
  }
  if (error != nullptr) {
    *error = 0;
  }

  if (args == nullptr || args->arg_count != 1 || args->args == nullptr ||
      args->args[0] == nullptr) {
    if (is_null != nullptr) {
      *is_null = 1;
    }
    return 0;
  }

  const char *s = args->args[0];
  unsigned long len = 0;
  if (args->lengths != nullptr) {
    len = args->lengths[0];
  } else {
    len = (unsigned long)strlen(s);
  }

  long long cnt = 0;
  bool in_word = false;
  for (unsigned long i = 0; i < len; i++) {
    unsigned char c = (unsigned char)s[i];
    if (isspace(c)) {
      in_word = false;
      continue;
    }
    if (!in_word) {
      cnt++;
      in_word = true;
    }
  }
  return cnt;
}

}  // extern "C"
