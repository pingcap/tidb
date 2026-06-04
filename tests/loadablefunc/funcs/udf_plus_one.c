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

#include <stdbool.h>
#include <string.h>

#include "udf_registration_types.h"

bool plus_one_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  (void)initid;
  if (args == NULL || args->arg_count != 1) {
    if (message != NULL) {
      const char *msg = "plus_one() requires exactly one argument";
      strncpy(message, msg, 255);
      message[255] = 0;
    }
    return true;
  }
  if (args->arg_type != NULL) {
    args->arg_type[0] = INT_RESULT;
  }
  return false;
}

void plus_one_deinit(UDF_INIT *initid) { (void)initid; }

long long plus_one(UDF_INIT *initid, UDF_ARGS *args, unsigned char *is_null,
                   unsigned char *error) {
  (void)initid;
  if (is_null != NULL) {
    *is_null = 0;
  }
  if (error != NULL) {
    *error = 0;
  }

  if (args == NULL || args->arg_count != 1 || args->args == NULL ||
      args->args[0] == NULL) {
    if (is_null != NULL) {
      *is_null = 1;
    }
    return 0;
  }

  // For INT_RESULT args, TiDB passes a pointer to long long.
  return *((long long *)args->args[0]) + 1;
}
