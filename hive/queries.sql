SET hive.execution.engine=mr;
SET hive.exec.mode.local.auto=true;
SET hive.auto.convert.join=false;
CREATE DATABASE IF NOT EXISTS banking_data;
USE banking_data;

-- Q1: quote-aware CSV staging, followed by a typed ORC client_info table.
DROP TABLE IF EXISTS raw_bank;
CREATE EXTERNAL TABLE raw_bank (
 age STRING, job STRING, marital STRING, education STRING, `default` STRING,
 balance STRING, housing STRING, loan STRING, contact STRING, day STRING,
 month STRING, duration STRING, campaign STRING, pdays STRING, previous STRING,
 poutcome STRING, y STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',', 'quoteChar'='"')
STORED AS TEXTFILE LOCATION '/banking/raw'
TBLPROPERTIES ('skip.header.line.count'='1');
DROP TABLE IF EXISTS client_info;
CREATE TABLE client_info STORED AS ORC AS SELECT
 CAST(age AS INT) age, job, marital, education, `default`,
 CAST(balance AS INT) balance, housing, loan, contact, CAST(day AS INT) day,
 month, CAST(duration AS INT) duration, CAST(campaign AS INT) campaign,
 CAST(pdays AS INT) pdays, CAST(previous AS INT) previous, poutcome, y FROM raw_bank
 WHERE age RLIKE '^[0-9]+$' AND y IN ('yes','no');
DESCRIBE client_info;

-- Q2
SELECT 'Q2_count' AS question,COUNT(*) AS clients FROM client_info;
SELECT * FROM client_info LIMIT 10;
-- Q3
SELECT * FROM client_info WHERE marital='married' AND loan='yes';
SELECT job,marital,balance FROM client_info ORDER BY balance DESC LIMIT 10;
-- Q4
SELECT job,COUNT(*) clients,AVG(age) avg_age FROM client_info GROUP BY job ORDER BY job;
SELECT education,COUNT(*) default_count FROM client_info WHERE `default`='yes' GROUP BY education ORDER BY education;
-- Q5
SELECT job,COUNT(*) clients,AVG(balance) avg_balance,100.0*SUM(CASE WHEN y='yes' THEN 1 ELSE 0 END)/COUNT(*) subscription_pct
FROM client_info GROUP BY job ORDER BY avg_balance DESC LIMIT 5;
SELECT month,COUNT(*) contacts,100.0*SUM(CASE WHEN y='yes' THEN 1 ELSE 0 END)/COUNT(*) subscription_pct
FROM client_info GROUP BY month ORDER BY contacts DESC LIMIT 1;
-- Q6
SELECT CORR(age,balance) age_balance_pearson FROM client_info;
-- Q7: no year exists. Report the limitation; month is not a substitute for year.
SELECT 'Year over year contact trends cannot be computed because no year or full date is supplied' AS q7_limitation;
-- Q8: balance is an average yearly balance, with no multiple-year time series.
SELECT education,COUNT(*) clients,AVG(balance) avg_yearly_balance,STDDEV_POP(balance) balance_sd,
       MIN(balance) minimum_balance,MAX(balance) maximum_balance
FROM client_info GROUP BY education ORDER BY avg_yearly_balance DESC;
-- Describe unusually large within-education balances using an explicit 3 SD heuristic.
SELECT education,COUNT(*) unusual_records FROM (
 SELECT education,balance,AVG(balance) OVER (PARTITION BY education) mu,
        STDDEV_POP(balance) OVER (PARTITION BY education) sd FROM client_info
) stats WHERE sd>0 AND ABS(balance-mu)>3*sd
GROUP BY education ORDER BY unusual_records DESC;
-- Q9
SELECT poutcome,COUNT(*) clients,100.0*SUM(CASE WHEN y='yes' THEN 1 ELSE 0 END)/COUNT(*) subscription_pct
FROM client_info GROUP BY poutcome ORDER BY subscription_pct DESC;
SELECT y,COUNT(*) clients,AVG(duration) avg_call_seconds FROM client_info GROUP BY y ORDER BY y;
