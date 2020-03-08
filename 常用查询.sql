-- 查询2019年三季度扣除非经常损益后的净资产收益率前100名，再从中筛选ttmpe大于0且小50的股票，同时显示股票名称和ttmpe 
select a.stockid, b.name, a.roe, a.roe_dt, c.ttmpe from 
(SELECT left(ts_code, 6) as stockid, roe, roe_dt FROM stockdata.fina_indicator where end_date='20190930' order by roe_dt desc limit 100) a,
(select stockid, name from stocklist) b,
(select aa.stockid as stockid, aa.ttmpe as ttmpe from klinestock aa, (select stockid, max(date) as maxdate from klinestock group by stockid) bb where aa.stockid=bb.stockid and aa.date=bb.maxdate) c
where a.stockid=b.stockid and a.stockid=c.stockid and c.ttmpe>0 and c.ttmpe<50;

-- 查询某天的评分， pe大于0， 稳定增长, pe200和pe1000小于5, 平均增长率大于20， 按评分倒序，pe正序排列
select * from valuation where date='20200228' and pe>0 and wdzz=1 and wdzz1=1 and pe200<5 and pe1000<5 and avg > 20 
order by pf desc, pe;

-- 近5年利润增长情况
select a.stockid, c.name, a.ttmprofits, b.ttmprofits, b.ttmprofits/a.ttmprofits as zz from 
(SELECT stockid, ttmprofits FROM stockdata.ttmlirun where date='20144' and ttmprofits>0) a,
(SELECT stockid, ttmprofits FROM stockdata.ttmlirun where date='20193' and ttmprofits>0) b,
stocklist c
where a.stockid=b.stockid and a.stockid=c.stockid having zz>10 order by zz desc;

-- 近5年某支股票利润增长情况
select a.stockid, c.name, round(a.ttmprofits / 10000 / 10000, 2) profits_a, round(b.ttmprofits / 10000 / 10000, 2) profits_b, 
	round(b.ttmprofits/a.ttmprofits, 2) as zz from 
(select stockid, ttmprofits from ttmlirun where stockid='000002' and date='20144') a,
(select stockid, ttmprofits from ttmlirun where stockid='000002' and date='20193') b,
stocklist c
where a.stockid=b.stockid and a.stockid=c.stockid order by zz desc;