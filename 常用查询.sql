-- 查询2019年三季度扣除非经常损益后的净资产收益率前100名，再从中筛选ttmpe大于0且小50的股票，同时显示股票名称和ttmpe 
select a.ts_code, b.name, a.roe, a.roe_dt, c.ttmpe from
(SELECT left(ts_code, 6) as ts_code, roe, roe_dt FROM stockdata.fina_indicator where end_date='20190930' order by roe_dt desc limit 100) a,
(select ts_code, name from stocklist) b,
(select aa.ts_code as ts_code, aa.ttmpe as ttmpe from klinestock aa, (select ts_code, max(date) as maxdate from klinestock group by ts_code) bb where aa.ts_code=bb.ts_code and aa.date=bb.maxdate) c
where a.ts_code=b.ts_code and a.ts_code=c.ts_code and c.ttmpe>0 and c.ttmpe<50;

-- 查询某天的评分， pe大于0， 稳定增长, pe200和pe1000小于5, 平均增长率大于20， 按评分倒序，pe正序排列
select * from valuation where date='20200228' and pe>0 and wdzz=1 and wdzz1=1 and pe200<5 and pe1000<5 and avg > 20 
order by pf desc, pe;

-- 近5年利润增长情况
select a.ts_code, c.name, a.ttmprofits, b.ttmprofits, b.ttmprofits/a.ttmprofits as zz from
(SELECT ts_code, ttmprofits FROM stockdata.ttmlirun where date='20144' and ttmprofits>0) a,
(SELECT ts_code, ttmprofits FROM stockdata.ttmlirun where date='20193' and ttmprofits>0) b,
stocklist c
where a.ts_code=b.ts_code and a.ts_code=c.ts_code having zz>10 order by zz desc;

-- 近5年某支股票利润增长情况
select a.ts_code, c.name, round(a.ttmprofits / 10000 / 10000, 2) profits_a, round(b.ttmprofits / 10000 / 10000, 2) profits_b,
	round(b.ttmprofits/a.ttmprofits, 2) as zz from 
(select ts_code, ttmprofits from ttmlirun where ts_code='000002' and date='20144') a,
(select ts_code, ttmprofits from ttmlirun where ts_code='000002' and date='20193') b,
stocklist c
where a.ts_code=b.ts_code and a.ts_code=c.ts_code order by zz desc;