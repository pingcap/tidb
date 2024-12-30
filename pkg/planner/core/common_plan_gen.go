package core

/*
	explain analyze format='unity_plan' select * from t;

	{
		"time_cost": 10.1,
		""
	}
*/
func (e *Explain) unityPlan(sql string) (string, error) {
	return "", nil
}
