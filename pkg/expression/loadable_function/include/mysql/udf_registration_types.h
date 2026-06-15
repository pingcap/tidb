#ifndef TIDB_UDF_REGISTRATION_TYPES_H
#define TIDB_UDF_REGISTRATION_TYPES_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

enum Item_result {
  INVALID_RESULT = -1,
  STRING_RESULT = 0,
  REAL_RESULT = 1,
  INT_RESULT = 2,
  ROW_RESULT = 3,
  DECIMAL_RESULT = 4
};

typedef struct UDF_ARGS {
  unsigned int arg_count;
  enum Item_result *arg_type;
  char **args;
  unsigned long *lengths;
  char *maybe_null;
  char **attributes;
  unsigned long *attribute_lengths;
} UDF_ARGS;

typedef struct UDF_INIT {
  bool maybe_null;
  unsigned int decimals;
  unsigned long max_length;
  char *ptr;
  bool const_item;
} UDF_INIT;

typedef void (*udf_deinit)(UDF_INIT *);
typedef bool (*udf_init)(UDF_INIT *, UDF_ARGS *, char *);
typedef double (*udf_double)(UDF_INIT *, UDF_ARGS *, unsigned char *,
                             unsigned char *);
typedef long long (*udf_longlong)(UDF_INIT *, UDF_ARGS *, unsigned char *,
                                  unsigned char *);
typedef char *(*udf_string)(UDF_INIT *, UDF_ARGS *, char *, unsigned long *,
                            unsigned char *, unsigned char *);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TIDB_UDF_REGISTRATION_TYPES_H

