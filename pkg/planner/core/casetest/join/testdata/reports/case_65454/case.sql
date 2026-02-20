select distinct length(t0.k3) as c0 from t0
join t2 on ((t0.k0 = t2.k0) and ((t0.k0 = t2.k0) and (t0.k0 != t2.k0)))
left join t4 on ((t0.k0 = t4.k0) and not (t0.k0 in ('s0')))
left join t1 on ((t0.k0 = t1.k0) and ((t4.k0 < t1.k0) and (t2.k0 != t1.k0)))
left join t3 on ((t0.k0 = t3.k0) and ((t0.k0 <= t3.k0) and (t2.k0 < t4.k0)))
where (not (t0.k0 in ('s85','s56','s87')) and not (t2.k0 in ((select t0.k0 as c0 from t0 where (t0.k3 = t0.k3)))));
