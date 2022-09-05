package parser

import "testing"

func BenchmarkParse(b *testing.B) {
	sqls := []string{
		"select * from t where a>10",
		"select * from t where a>10 and b<20",
		"select * from t where a>10 and b<20 and c=11",
		"select a, b, c, d from t where a>10 and b<20 and c=11",
		"select * from t where a+10>10 and b<20 and c=11",
		"select * from t, tx where a>10 and b<20 and c=11",
	}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			_, _, _ = parser.Parse(sql, "", "")
		}
	}
}

func BenchmarkParameterize(b *testing.B) {
	sqls := []string{
		"select * from t where a>10",
		"select * from t where a>10 and b<20",
		"select * from t where a>10 and b<20 and c=11",
		"select a, b, c, d from t where a>10 and b<20 and c=11",
		"select * from t where a+10>10 and b<20 and c=11",
		"select * from t, tx where a>10 and b<20 and c=11",
	}
	parameterizer := NewParameterizer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			_, _, _, _ = parameterizer.Parse(sql, "", "")
		}
	}
}

func BenchmarkNormalize(b *testing.B) {
	sqls := []string{
		"select * from t where a>10",
		"select * from t where a>10 and b<20",
		"select * from t where a>10 and b<20 and c=11",
		"select a, b, c, d from t where a>10 and b<20 and c=11",
		"select * from t where a+10>10 and b<20 and c=11",
		"select * from t, tx where a>10 and b<20 and c=11",
	}
	d := digesterPool.Get().(*sqlDigester)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			_ = d.doNormalize(sql)
		}
	}
	b.StopTimer()
	digesterPool.Put(d)
}
