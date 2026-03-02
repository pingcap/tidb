# WORK14: 零分配 Time.AppendString — 消除 DumpTextRow 中 datetime 格式化的全部内存分配

## 背景

DumpTextRow（column.go:202）将 datetime 列序列化为 MySQL text protocol 时的调用链：

```
DumpTextRow [110.80s, 25.82%]
  └→ hack.Slice(row.GetTime(i).String())
       └→ Time.String() [79.26s, 18.47%]
            ├→ Time.DateFormat("%Y-%m-%d %H:%i:%s") [78.10s]
            │   └→ bytes.Buffer 分配
            │   └→ convertDateFormat() [53.40s]
            │       └→ FormatIntWidthN() × 6 [32.94s]
            │           ├→ make([]byte, n-len) — per-call 分配
            │           ├→ string(padBytes) + numString — 字符串拼接
            │           └→ strconv.FormatInt — 整数→字符串
            └→ fmt.Sprintf(".%06d", microsecond) — FSP 微秒格式化
```

FormatIntWidthN（time.go:2904）是核心瓶颈：
```go
func FormatIntWidthN(num, n int) string {
    numString := strconv.FormatInt(int64(num), 10)  // 分配 string
    padBytes := make([]byte, n-len(numString))      // 分配 []byte
    for i := range padBytes { padBytes[i] = '0' }
    return string(padBytes) + numString              // 拼接分配 string
}
```

每个 datetime 值（格式 %Y-%m-%d %H:%i:%s）调用 6 次 FormatIntWidthN（年×1、月×1、日×1、时×1、分×1、秒×1），每次 2-3 次内存分配。

## 修改范围

- pkg/types/time.go
- pkg/server/internal/column/column.go
- pkg/types/time_test.go（新增测试 + benchmark）

## 阅读范围

- pkg/types/time.go：String()(375)、DateFormat(2758)、convertDateFormat(2785)、FormatIntWidthN(2904)、Type()(296)、Fsp()(307)
- pkg/types/core_time.go：Year()(45)、Month()(59)、Day()(73)、Hour()(87)、Minute()(101)、Second()(115)、Microsecond()(129)
- pkg/server/internal/column/column.go：DumpTextRow(150)
- pkg/server/internal/dump/dump.go：LengthEncodedString(47)

## 实现方案

### 1) 新增零分配辅助函数

在 pkg/types/time.go 中新增：

```go
// appendInt2 appends a 2-digit zero-padded integer to buf.
func appendInt2(buf []byte, v int) []byte {
    return append(buf, byte('0'+v/10), byte('0'+v%10))
}

// appendInt4 appends a 4-digit zero-padded integer to buf.
func appendInt4(buf []byte, v int) []byte {
    return append(buf,
        byte('0'+v/1000),
        byte('0'+(v/100)%10),
        byte('0'+(v/10)%10),
        byte('0'+v%10),
    )
}
```

### 2) 新增 Time.AppendString 方法

```go
// AppendString appends the formatted time string to buf and returns the result.
// Zero allocations — all formatting is done via direct byte writes.
func (t Time) AppendString(buf []byte) []byte {
    buf = appendInt4(buf, t.Year())
    buf = append(buf, '-')
    buf = appendInt2(buf, t.Month())
    buf = append(buf, '-')
    buf = appendInt2(buf, t.Day())

    if t.Type() == mysql.TypeDate {
        return buf
    }

    buf = append(buf, ' ')
    buf = appendInt2(buf, t.Hour())
    buf = append(buf, ':')
    buf = appendInt2(buf, t.Minute())
    buf = append(buf, ':')
    buf = appendInt2(buf, t.Second())

    fsp := t.Fsp()
    if fsp > 0 {
        buf = append(buf, '.')
        micro := t.Microsecond()
        // Write fsp digits of the 6-digit microsecond value.
        digits := [6]byte{
            byte('0' + micro/100000),
            byte('0' + (micro/10000)%10),
            byte('0' + (micro/1000)%10),
            byte('0' + (micro/100)%10),
            byte('0' + (micro/10)%10),
            byte('0' + micro%10),
        }
        buf = append(buf, digits[:fsp]...)
    }
    return buf
}
```

### 3) 重写 Time.String() 内部实现

```go
func (t Time) String() string {
    // Pre-allocate: "YYYY-MM-DD HH:MM:SS.ffffff" max 26 bytes
    buf := make([]byte, 0, 26)
    buf = t.AppendString(buf)
    return string(buf)
}
```

这样 String() 从原来的 ~18 次分配降为 1 次分配（仅 make + string() 转换）。

### 4) 修改 DumpTextRow 使用 AppendString

在 pkg/server/internal/column/column.go：

```go
// 同时将 tmp 初始容量从 20 增加到 32，以容纳 datetime 字符串（最长 26 字节）
tmp := make([]byte, 0, 32)

// ...

case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
    // before: buffer = dump.LengthEncodedString(buffer, hack.Slice(row.GetTime(i).String()))
    tmp = row.GetTime(i).AppendString(tmp[:0])
    buffer = dump.LengthEncodedString(buffer, tmp)
```

这彻底消除了 datetime 输出路径中 String() → hack.Slice() 的中间字符串分配。

## 必须新增的测试

在 pkg/types/time_test.go 新增：

- **TestTimeAppendString** — 验证 AppendString 与 String() 输出一致：
  - Date 类型："2024-01-02"
  - Datetime 类型 FSP=0："2024-01-02 03:04:05"
  - Timestamp 类型 FSP=3："2024-01-02 03:04:05.123"
  - FSP=6："2024-01-02 03:04:05.123456"
  - 零值："0000-00-00" / "0000-00-00 00:00:00"
  - 边界月日：月=1、月=12、日=1、日=31
- **BenchmarkTimeString / BenchmarkTimeAppendString** — 对比优化前后：
  - Date 类型
  - Datetime FSP=0
  - Datetime FSP=6

## 需要运行的测试

```bash
# 正确性
go test ./pkg/types -run 'TestTimeAppendString|TestFormatIntWidthN|TestDateFormat' -count=1
go test ./pkg/types -count=1
go test ./pkg/server/internal/column -count=1

# benchmark（改前改后对比）
go test ./pkg/types -run '^$' -bench 'BenchmarkTime(String|AppendString)' -benchmem -count=5

# 集成回归
go test ./pkg/server -run 'TestDump' -count=1
```

## 验收标准

- BenchmarkTimeAppendString allocs/op = 0（零分配）
- BenchmarkTimeString（重写后）allocs/op = 1（仅 make + string 转换）
- 对比原 BenchmarkTimeString allocs/op 显著减少（原约 12-18 allocs/op）
- TestTimeAppendString 通过，输出与 String() 一致
- 所有已有测试通过
