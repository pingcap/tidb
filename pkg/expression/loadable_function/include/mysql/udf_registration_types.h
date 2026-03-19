#ifndef TIDB_UDF_REGISTRATION_TYPES_H
#define TIDB_UDF_REGISTRATION_TYPES_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Item_result matches MySQL's UDF argument/result type enum.
// Ref: https://dev.mysql.com/doc/extending-mysql/8.4/en/adding-loadable-function.html
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
  void *extension;
} UDF_ARGS;

typedef struct UDF_INIT {
  bool maybe_null;
  unsigned int decimals;
  unsigned long max_length;
  char *ptr;
  bool const_item;
  void *extension;
} UDF_INIT;

typedef void (*Udf_func_deinit)(UDF_INIT *);
typedef bool (*Udf_func_init)(UDF_INIT *, UDF_ARGS *, char *);
typedef double (*Udf_func_double)(UDF_INIT *, UDF_ARGS *, unsigned char *,
                                  unsigned char *);
typedef long long (*Udf_func_longlong)(UDF_INIT *, UDF_ARGS *, unsigned char *,
                                       unsigned char *);
typedef char *(*Udf_func_string)(UDF_INIT *, UDF_ARGS *, char *, unsigned long *,
                                 unsigned char *, unsigned char *);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // TIDB_UDF_REGISTRATION_TYPES_H

