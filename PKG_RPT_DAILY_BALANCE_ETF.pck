CREATE OR REPLACE PACKAGE PKG_RPT_DAILY_BALANCE_ETF IS
--2021-10-28
--Target table: TTRD_ACC_ETF_BALANCE
--Purpose: Calculate ETF subscription/redemption profit and loss
PROCEDURE P_RPT_DAILY_BALANCE_ETF(P_BEG_DATE    IN VARCHAR2,
                         P_END_DATE    IN VARCHAR2,
                         P_RESULT      OUT VARCHAR2, --Execution result
                         P_RESULT_INFO OUT VARCHAR2 --Execution details
                         );

END PKG_RPT_DAILY_BALANCE_ETF;

/*Script*/
--Log query
--SELECT * FROM TTRD_PROC_RUN_LOG WHERE PROC_NAME='PKG_RPT_DAILY_BALANCE_ETF';
--Summary verification

 --Summary verification
/*  SELECT HT.BASE_DATE,
       --stkid,
      -- zqdm,
       S.QSRQ,
       HT.SECU_AMOUNT - S.GPYE2 AS Position_Difference,
       HT.SECU_COST - S.GPJE2 AS Cost_Difference,
       --(HT.TRD) - (S.SYE) AS Trading_PL_Difference,
       (HT.sye) - (S.SYE) AS Realized_PL_Difference,
       HT.SECU_MK - S.fdje2 AS Market_Value_Difference,
       HT.COMMISION - S.sxf AS Commission_Difference,
       --ht.etf_xjce-s.etf_xjce as Cash_Difference_Difference,
       --ht.etf_xjfh-s.etf_xjfh as Cash_Return_Difference,
       HT.ETF_CASH - S.etf_cash AS Cash_Settlement_Difference,
       HT.SECU_AMOUNT,
       S.GPYE2,
       HT.SECU_COST,
       S.GPJE2,
       HT.ETF_CASH,
       HT.TRD,
       S.SYE,
       HT.IR
  FROM (SELECT BASE_DATE,
               --stkid,
               SUM(NVL(SECU_AMOUNT, 0)) AS SECU_AMOUNT,
               SUM(NVL(SECU_COST, 0)) AS SECU_COST,
               SUM(NVL(SECU_MK, 0)) AS SECU_MK,
               SUM(NVL(D_FV, 0)) AS D_FV,
               SUM(NVL(SECU_TRD, 0)\* + NVL(ETF_AMT, 0) + NVL(ETF_XJTD, 0)*\) AS TRD,
               SUM(NVL(SECU_TRD, 0) + NVL(ETF_AMT, 0) + NVL(ETF_XJTD, 0)) AS sye,
               SUM(NVL(COMMISION, 0)) AS COMMISION,
               SUM(NVL(IR, 0)) AS IR,
               SUM(NVL(ETF_XJCE, 0) + NVL(ETF_XJFH, 0)) AS ETF_CASH,
               SUM(ETF_AMT) as ETF_AMT,
               SUM(ETF_XJTD) as ETF_XJTD,
               SUM(ETF_XJCE) as ETF_XJCE,
               SUM(ETF_XJFH) as ETF_XJFH,
               SUM(D_PNL) AS D_PNL

          FROM TTRD_ACC_ETF_BALANCE
         WHERE BASE_DATE BETWEEN '2021-01-15' AND '2021-10-31' --AND stkid IN('159970')
         --and BASE_DATE='2021-08-16' and (ETF_XJCE<>0 or ETF_XJfh<>0)
         GROUP BY \*stkid, *\BASE_DATE) HT
  LEFT JOIN (
             select
             nvl(s.QSRQ,ls.QSRQ) as QSRQ,
             GPYE2,GPJE2,FDJE2,SYE,
             etf_cash,
             --etf_xjce,
             --etf_xjfh,
             sxf
             from(
             SELECT TO_CHAR(QSRQ, 'yyyy-mm-dd') AS QSRQ,
                    --ZQDM,
                    SUM(GPYE2) AS GPYE2,
                    SUM(GPJE2) AS GPJE2,
                    SUM(FDJE2) AS FDJE2,
                    SUM(SYE) AS SYE
               FROM XIR_ZT.SONG_GPJSKLS
              WHERE QSRQ BETWEEN DATE '2021-01-15' AND DATE'2021-10-31' --AND ZQDM IN('159970')
             -- and QSRQ=date'2021-08-13'
                AND zjzh = '0001100000000515'
                AND ZQLBMC NOT IN ('Bond Repo', 'Special Business')
              GROUP BY QSRQ\*, ZQDM*\)s
              full join (
              select TO_CHAR(QSRQ, 'yyyy-mm-dd') as QSRQ,--zqdm,--ZYMC,
              --sum(sxf)
              sum(case when trim(ZYMC)='ETF Cash' then sxf else 0 end ) as etf_cash,
             -- sum(case when trim(ZYMC)='ETF Cash' and trim(regexp_replace(BZXX,'[0-9]'))='ETF Cash Substitution, Cash Difference' then sxf else 0 end ) as etf_xjce,
              --sum(case when trim(ZYMC)='ETF Cash' and trim(regexp_replace(BZXX,'[0-9]'))='ETF Refund-' then sxf else 0 end ) as etf_xjfh,
              sum(case when trim(ZYMC) in('ETF Subscription','ETF Redemption','Normal','Other Business') then abs(sxf) else 0 end ) as sxf
                from xir_zt.song_dzdkls
               where zjzh = '0001100000000515' --and ZQDM IN ('159922') and QSRQ = DATE '2021-01-05'
                 and ZQLBMC NOT IN ('Bond Repo', 'Special Business')
                 and QSRQ BETWEEN DATE '2021-02-22' AND DATE'2021-02-22'
                -- and QSRQ=date'2021-08-13'
                 and sxf<>0
                 group by QSRQ--,zqdm--,ZYMC
              )ls
              on s.QSRQ=ls.QSRQ --and s.zqdm=ls.zqdm
) S
    ON S.QSRQ = HT.BASE_DATE
    order by 1;*/
/
CREATE OR REPLACE PACKAGE BODY PKG_RPT_DAILY_BALANCE_ETF IS

--2021-10-28
--Target table: TTRD_ACC_ETF_BALANCE
--Purpose: Calculate ETF subscription/redemption profit and loss

/*
       Date: 2021-11-06
       Author: Huang Xiaosha
       Description: 1) Add spot account for options;

       Date: 2022-02-17
       Author: Huang Xiaosha
       Description: 1) Correct exercise sell profit/loss;
       
       Date: 2022-08-06
       Author: Tian Fangning
       Description: 1) Correct spot profit/loss, add subscription scenario
             2) Since 560010 code changes to 560013 during subscription period, code conversion is needed;
             
       Date: 2022-09-06
       Author: Tian Fangning
       Description: Add share merge/split scenarios
			 
			 
	   Date: 2022-09-19
	   Author: Tian Fangning
	   Description: Split spot account 888000000009 into three accounts ('159919','888000000009','159922','888000000096','159915','888000000098')
            
*/

PROCEDURE P_RPT_DAILY_BALANCE_ETF(P_BEG_DATE    IN VARCHAR2,
                         P_END_DATE    IN VARCHAR2,
                         P_RESULT      OUT VARCHAR2, --Execution result
                         P_RESULT_INFO OUT VARCHAR2 --Execution details
                         ) IS
/*
Source tables: XIR_TRD.VG_TRADINGLOG--Genius trading log
        XIR_TRD.VG_STKINFO--Genius market data
        CTSDB.BRIEFDEFINE@CTSSP--Genius instruction types
        CTSDB.STKTYPE@CTSSP--Genius asset types
Target table: XIR_TRD.TTRD_ACC_ETF_BALANCE  --ETF profit/loss table
Calculation sequence:
ETF: 1. Subscription/Buy 2. Redemption/Sell
Stock: 1. Redemption/Buy 2. Subscription/Sell
Calculations: Trading, subscription/redemption, cash return settlement, cash difference settlement, cash substitution amount deduction, dividends, securities transfer in/out (fund split)
Involved BRIEFIDs:
'005_002_025','005_002_002','005_001_002'--Redemption cash substitution, subscription cash substitution
'005_002_001',--Sell execution
'005_001_001',--Buy execution
'005_001_003',--Subscription/redemption share increase
'005_002_003',--Subscription/redemption share decrease
'005_001_011',--Cash difference settlement decrease
'005_002_011',--Cash difference settlement increase
'005_001_012',--Cash substitution settlement decrease
'005_002_012'--Cash substitution settlement increase
'005_005_001'--Dividends, interest
005_003_003 Bonus share transfer in
005_004_002 Securities transfer out
005_004_003 Securities transfer in--Fund split
005_003_005 Non-tradable transfer in
005_004_003 Non-tradable transfer out--Listing
005_005_069 Combination fee/depository fee--Added 2021-12-03, not synchronized in test
'005_001_006' Subscription lottery --Added 2022-08-06
'005_004_039','Rights offering/placement transfer out (share merge)'--Added 20220906
'005_003_004','Rights offering/placement transfer in (share split)'--Added 20220906
Special notes:
Genius cash difference only appears in RECKONINGTIME (settlement date) log, can't get this data on T+1, so we take subscription/redemption list data
Cash difference from subscription/redemption list is written to ETF_XJCE_QD field with SH +1, SZ +2
*/
V_BASE_DATE VARCHAR2(10);--Current date
V_PR1_DATE VARCHAR2(10);--Previous trading day
V_PR2_DATE VARCHAR2(10);--2 trading days ago
V_QTY NUMBER;--Position
V_COST NUMBER;--Cost
V_COMMISION NUMBER;--Fee
V_UNIT_COST NUMBER;--Unit cost
V_SECU_TRD NUMBER;--Price difference income
/*V_ETF_XJCE NUMBER;--Cash difference
V_ETF_XJFH NUMBER;--Cash return
V_ETF_XJTD NUMBER;--Cash substitution*/
V_ETF_AMT NUMBER;--Subscription/redemption market value
V_UPDATE_CNT NUMBER;--Records updated this time
V_MODULE_NAME VARCHAR2(500); --Module name
CURSOR TRD_CURSOR--Read today's trading log
IS
  --Genius trading log information
SELECT V_BASE_DATE AS BASE_DATE, --Clearing time
       --TO_CHAR(TO_DATE(TRD.RECKONINGTIME, 'yyyymmdd'), 'yyyy-mm-dd') RECKONINGTIME, --Settlement time
       CASE WHEN TRD.ACCTID='888000000009' 
									THEN decode(trd.stkid,'159919','888000000009','159922','888000000096','159915','888000000098' ) 
                 ELSE  TRD.ACCTID
       END AS ACCTID, --0919 fund account
       BF.EXTERIORDESC, --Instruction description
       TRD.BRIEFID, --Instruction
       TRD.EXCHID, --Market
       decode(TRD.STKID,'560013','560010',TRD.STKID) as STKID, --Contract code
       TRD.STKTYPE, --Contract type
       STKTYPE.STKTYPENAME AS STKTYPENAME, --Type description
       STKINFO.NEWPRICE,
       TRD.KNOCKQTY, --Execution quantity
       --Subscription/redemption processed as market value, others as execution amount
       (CASE WHEN TRD.BRIEFID IN('005_002_003'/*Share decrease*/,'005_001_003'/*Share increase*/) THEN STKINFO.NEWPRICE*TRD.KNOCKQTY
       ELSE TRD.KNOCKAMT END )AS KNOCKAMT, --Execution amount (excluding fee)
       TRD.RECKONINGAMT, --Clearing amount (including fee)
       ABS(TRD.COMMISION) AS COMMISION --Fee
FROM  (SELECT V_BASE_DATE AS OCCURTIME, --Occurrence time
               --SUBSTR(TRD.RECKONINGTIME, 1, 8) AS RECKONINGTIME, --Settlement time
               TRD.ACCTID, --Fund account
               TRD.BRIEFID, --Instruction
               TRD.EXCHID, --Market
               TRD.STKID, --Contract code
               TRD.STKTYPE, --Contract type
               SUM(TRD.KNOCKQTY) AS KNOCKQTY, --Execution quantity
               SUM(TRD.KNOCKAMT) AS KNOCKAMT, --Execution amount (excluding fee)
               SUM(TRD.RECKONINGAMT) AS RECKONINGAMT, --Clearing amount (including fee)
               SUM(ABS(TRD.RECKONINGAMT)) - SUM(TRD.KNOCKAMT) AS COMMISION --Fee
        FROM   XIR_TRD.VG_TRADINGLOG TRD--Genius trading log table
        WHERE  1 = 1
        /*Test*/ --AND STKID='510300'
        AND  (TRD.ACCTID = '100000000515'
                  OR    (CASE WHEN TRD.ACCTID='888000000009' 
									THEN decode(trd.stkid,'159919','888000000009','159922','888000000096','159915','888000000098' ) END)
									IN (SELECT F5 FROM  XIR_TRD.TTRD_EXT_PROPERTY_DATA D WHERE D.SOURCE_TYPE = 'EXT_Derivatives_Department_PL_Report_Account' AND D.F8 = 'Yes'  AND D.F4 = 'Spot'))--2021-11-05 Add options spot account
        --and SUBSTR(TRD.RECKONINGTIME, 1, 8)=REPLACE(V_BASE_DATE,'-')--Settlement date
        AND (
         --condition 1
         ((SUBSTR(TRD.OCCURTIME, 1, 8) =REPLACE(V_BASE_DATE,'-') AND TRD.BRIEFID NOT IN('005_001_011','005_002_011'))
          AND NOT((TRD.BRIEFID  IN('005_001_002','005_002_002')/*Subscription/redemption*/ AND TRD.EXCHID='1' AND  TRD.STKTYPE='A8' AND  TRD.KNOCKQTY<>0)
          or( TRD.BRIEFID  IN('005_001_012','005_002_012') /*Cash substitution return*/AND TRD.EXCHID='1')))
         --condition 2
        OR (SUBSTR(TRD.OCCURTIME, 1, 8) =REPLACE(V_PR1_DATE,'-') AND TRD.BRIEFID  IN('005_001_011','005_002_011') AND TRD.EXCHID='0')/*SH take previous day's cash difference*/
        --condition 3
        OR (SUBSTR(TRD.OCCURTIME, 1, 8) =REPLACE(V_BASE_DATE,'-') AND TRD.BRIEFID  IN('005_001_002','005_002_002')/*Subscription/redemption*/ AND TRD.EXCHID='1' AND TRD.STKTYPE='A8' AND TRD.KNOCKQTY<>0)/*2022-06-20 Take same day SZSE subscription/redemption fee*/
        --condition 4
        OR (SUBSTR(TRD.OCCURTIME, 1, 8) =REPLACE(V_PR2_DATE,'-') AND TRD.BRIEFID  IN('005_001_011','005_002_011') AND TRD.EXCHID='1')/*SZ take 2 days ago cash difference*/
        --condition 5
        OR (SUBSTR(TRD.RECKONINGTIME, 1, 8) =REPLACE(V_BASE_DATE,'-') AND TRD.BRIEFID  IN('005_001_012','005_002_012') AND TRD.EXCHID='1')/*Take SZ cash substitution settlement*/
        )
        GROUP  BY --SUBSTR(TRD.OCCURTIME, 1, 8), --Occurrence time
                  --SUBSTR(TRD.RECKONINGTIME, 1, 8), --Settlement time
                  TRD.ACCTID, --Fund account
                  TRD.BRIEFID, --Instruction
                  TRD.EXCHID, --Market
                  TRD.STKID, --Contract code
                  TRD.STKTYPE --Contract type
        ) TRD
INNER   JOIN CTSDB.BRIEFDEFINE@CTSSP BF
ON     TRD.BRIEFID = BF.BRIEFID
INNER   JOIN CTSDB.STKTYPE@CTSSP STKTYPE
ON     TRD.STKTYPE = STKTYPE.STKTYPE
LEFT  JOIN(SELECT STKID,
            STKTYPE,
            EXCHID,
            V_BASE_DATE AS OCCURTIME,
            NEWPRICE
     FROM   XIR_TRD.VG_STKINFO--Genius market data table
     WHERE  SUBSTR(OCCURTIME, 1, 8) = REPLACE(V_BASE_DATE,'-')) STKINFO
ON     TRD.EXCHID = STKINFO.EXCHID
AND    TRD.STKTYPE = STKINFO.STKTYPE
AND    TRD.STKID = STKINFO.STKID
AND    TRD.OCCURTIME = STKINFO.OCCURTIME
ORDER BY ACCTID,STKID,STKTYPE,BRIEFID;
  --Today's position information
CURSOR POS_CURSOR(TRD_BASE_DATE VARCHAR2,TRD_ACCTID VARCHAR2,TRD_STKID VARCHAR2, TRD_STKTYPE VARCHAR2, TRD_EXCHID VARCHAR2,TRD_BRIEFID VARCHAR2)
IS
  SELECT *
  FROM TTRD_ACC_ETF_BALANCE B
  WHERE B.ACCTID=TRD_ACCTID
  AND B.STKTYPE=TRD_STKTYPE
  AND B.EXCHID=TRD_EXCHID
  AND B.STKID=TRD_STKID
  AND B.BASE_DATE=TRD_BASE_DATE
  AND TRD_BRIEFID IN('005_002_001','005_002_003');--Sell\Share decrease

BEGIN
    FOR CR IN (SELECT CAL_DAY AS BASE_DATE
               FROM XIR_MD.TCALENDAR_DATES T--Calendar table
               WHERE T.CAL_CODE = 'CHINA_EX' --Exchange
               AND T.CAL_FLAG = 1 --Trading day
               AND T.CAL_DAY BETWEEN P_BEG_DATE AND P_END_DATE) LOOP
--Current processing date
--Can only run from '2021-01-05'
if CR.BASE_DATE>='2021-01-05' then
V_BASE_DATE:=CR.BASE_DATE;
end if;
V_PR1_DATE:=xir_md.get_pretradedays('CHINA_EX',V_BASE_DATE,1);--Previous trading day
V_PR2_DATE:=xir_md.get_pretradedays('CHINA_EX',V_BASE_DATE,2);--2 trading days ago

--Delete today's data
DELETE FROM TTRD_ACC_ETF_BALANCE WHERE BASE_DATE=V_BASE_DATE;COMMIT;
V_MODULE_NAME:='Smooth yesterday''s position';
--Smooth yesterday's position data to today
INSERT INTO TTRD_ACC_ETF_BALANCE
(BASE_DATE,
ACCTID,
EXCHID,
STKID,
STKTYPE,
STKTYPENAME,
NEWPRICE,
SECU_AMOUNT,
SECU_COST,
SECU_MK)
SELECT V_BASE_DATE AS BASE_DATE,
      B.ACCTID,
      B.EXCHID,
      decode(B.STKID,'560013','560010',B.STKID) as STKID,
      B.STKTYPE,
      B.STKTYPENAME,
      STKINFO.NEWPRICE,
      B.SECU_AMOUNT,
      B.SECU_COST,
      STKINFO.NEWPRICE*B.SECU_AMOUNT AS SECU_MK
FROM (SELECT BASE_DATE, ACCTID, EXCHID, DECODE(STKID,'560013','560010',STKID)AS STKID, STKTYPE, STKTYPENAME, NEWPRICE, SECU_AMOUNT, SECU_COST, SECU_MK, COMMISION, SECU_TRD, ETF_XJCE, ETF_XJFH, ETF_XJTD, ETF_AMT, D_FV, D_PNL, CREATE_TIME, IR, FAIR_PRICE, ETF_XJCE_QD
     FROM  TTRD_ACC_ETF_BALANCE
     WHERE  BASE_DATE = V_PR1_DATE
     AND SECU_AMOUNT<>0
) B --20220411 Optimize query
INNER  JOIN(SELECT  case when SUBSTR(OCCURTIME, 1, 8)>= REPLACE('2022-08-03','-')   then stkid 
            else decode(STKID,'560013','560010',STKID) end AS STKID,--Since Genius has both 560010 and 560013 data after listing, handle this way
            STKTYPE,
            EXCHID,
            V_BASE_DATE AS OCCURTIME,
            NEWPRICE
     FROM   XIR_TRD.VG_STKINFO
     WHERE  SUBSTR(OCCURTIME, 1, 8) = REPLACE(V_BASE_DATE,'-')
     ) STKINFO
ON     B.EXCHID = STKINFO.EXCHID
AND    B.STKTYPE = STKINFO.STKTYPE
AND   B.STKID = STKINFO.STKID
--AND    B.BASE_DATE = STKINFO.OCCURTIME
;
COMMIT;

----Process today's trading log
FOR TRD_ROW IN TRD_CURSOR LOOP
/*01
First process as buy:
'005_001_001','Buy',
'005_001_003','Subscription/redemption share increase',
'005_001_006' ,'Subscription lottery'
*/

IF TRD_ROW.BRIEFID IN ('005_001_001','005_001_003','005_001_006') THEN
V_MODULE_NAME:='Process as buy';
--Test output
--DBMS_OUTPUT.PUT_LINE(TRD_ROW.TRDNAME||TRD_ROW.STKID||'-------'||TRD_ROW.KNOCKQTY||'++++++'||TRD_ROW.KNOCKAMT);
--Buy: update quantity, cost, fee, market value, closing price
MERGE INTO TTRD_ACC_ETF_BALANCE S
USING(SELECT V_BASE_DATE AS BASE_DATE,
TRD_ROW.ACCTID AS ACCTID,
TRD_ROW.STKID AS STKID,
TRD_ROW.STKTYPE AS STKTYPE,
TRD_ROW.STKTYPENAME AS STKTYPENAME,
TRD_ROW.EXCHID AS EXCHID,
TRD_ROW.KNOCKQTY AS SECU_AMOUNT,
TRD_ROW.KNOCKAMT  AS SECU_COST,
TRD_ROW.COMMISION AS COMMISION,
TRD_ROW.NEWPRICE AS NEWPRICE,
(CASE WHEN TRD_ROW.BRIEFID IN( '005_001_003')THEN TRD_ROW.KNOCKAMT ELSE 0 END )  AS ETF_AMT,
TRD_ROW.KNOCKQTY*TRD_ROW.NEWPRICE AS SECU_MK
FROM DUAL
)A
  ON( A.BASE_DATE=S.BASE_DATE
  AND A.ACCTID=S.ACCTID
  AND A.STKID=S.STKID
  AND A.STKTYPE=S.STKTYPE
  AND A.EXCHID=S.EXCHID)
WHEN MATCHED THEN
UPDATE SET
S.STKTYPENAME=A.STKTYPENAME,
S.SECU_AMOUNT=NVL(S.SECU_AMOUNT,0)+A.SECU_AMOUNT,
S.SECU_COST=NVL(S.SECU_COST,0)+A.SECU_COST,
S.COMMISION=NVL(S.COMMISION,0)+A.COMMISION,
S.NEWPRICE=A.NEWPRICE,
S.ETF_AMT=NVL(S.ETF_AMT,0)+A.ETF_AMT,
S.SECU_MK=(NVL(S.SECU_AMOUNT,0)+A.SECU_AMOUNT)*A.NEWPRICE
WHEN NOT MATCHED THEN INSERT
(BASE_DATE, ACCTID, STKID, STKTYPE,STKTYPENAME, EXCHID, SECU_AMOUNT, SECU_COST,COMMISION, ETF_AMT,NEWPRICE,SECU_MK)
VALUES(A.BASE_DATE, A.ACCTID, A.STKID, A.STKTYPE,A.STKTYPENAME, A.EXCHID, A.SECU_AMOUNT, A.SECU_COST,A.COMMISION, A.ETF_AMT,A.NEWPRICE,A.SECU_MK);
COMMIT;
END IF;

/*02
Process as sell:
'005_002_001','Sell',
'005_002_003','Subscription/redemption share decrease'
*/
IF  TRD_ROW.BRIEFID IN ('005_002_001','005_002_003') THEN
  V_MODULE_NAME:='Process as sell';
  FOR POS_ROW IN POS_CURSOR(TRD_ROW.BASE_DATE,TRD_ROW.ACCTID,TRD_ROW.STKID, TRD_ROW.STKTYPE, TRD_ROW.EXCHID,TRD_ROW.BRIEFID) LOOP
--Test output
--DBMS_OUTPUT.PUT_LINE('Position before sell'||TRD_ROW.STKID||'-------'||POS_ROW.SECU_AMOUNT||'-----'||POS_ROW.SECU_COST);
    --IF TRD_ROW.BRIEFID NOT IN('229','230','231','232') THEN
    V_QTY:=NVL(POS_ROW.SECU_AMOUNT,0);
    V_COST:=NVL(POS_ROW.SECU_COST,0);
    V_COMMISION:=NVL(POS_ROW.COMMISION,0);
    V_UNIT_COST:=/*round(*/NVL(V_COST/NULLIF(V_QTY,0),0)/*,18)*/;--Debug precision
    V_SECU_TRD:=NVL(POS_ROW.SECU_TRD,0);
    V_ETF_AMT:=NVL(POS_ROW.ETF_AMT,0);
    --END IF;
--Test output
--DBMS_OUTPUT.PUT_LINE('Assigned values'||TRD_ROW.STKID||'-----'||V_QTY||'-------'||V_COST||'-------'||V_UNIT_COST);
V_QTY:=NVL(V_QTY,0)-NVL(TRD_ROW.KNOCKQTY,0);
V_COST:=/*V_COST-*/NVL(V_UNIT_COST,0)*NVL(V_QTY,0);
V_COMMISION:=NVL(V_COMMISION,0)+NVL(TRD_ROW.COMMISION,0);
V_UNIT_COST:=NVL(V_UNIT_COST,0);
V_SECU_TRD:=NVL(V_SECU_TRD,0)+(NVL(TRD_ROW.KNOCKAMT,0)-NVL(V_UNIT_COST,0)*NVL(TRD_ROW.KNOCKQTY,0));
V_ETF_AMT:=V_ETF_AMT+(CASE WHEN TRD_ROW.BRIEFID IN( '005_002_003') THEN -1*TRD_ROW.KNOCKAMT ELSE 0 END);

--Test output
--DBMS_OUTPUT.PUT_LINE('After sell processing'||V_QTY||'-------'||V_COST||'-------'||V_SECU_TRD);

--Sell: update quantity, cost, commission, price difference income, market value, closing price
MERGE INTO TTRD_ACC_ETF_BALANCE S
USING(SELECT V_BASE_DATE AS BASE_DATE,
TRD_ROW.ACCTID AS ACCTID,
TRD_ROW.STKID AS STKID,
TRD_ROW.STKTYPE AS STKTYPE,
TRD_ROW.STKTYPENAME AS STKTYPENAME,
TRD_ROW.EXCHID AS EXCHID,
V_QTY AS SECU_AMOUNT,
/*ROUND(*/V_COST/*,2)*/ AS SECU_COST,
NVL(V_SECU_TRD,0) AS SECU_TRD,
NVL(V_COMMISION,0) AS COMMISION,
V_ETF_AMT AS ETF_AMT,
TRD_ROW.NEWPRICE AS NEWPRICE,
V_QTY*TRD_ROW.NEWPRICE AS SECU_MK
FROM DUAL
)A
  ON( A.BASE_DATE=S.BASE_DATE
  AND A.ACCTID=S.ACCTID
  AND A.STKID=S.STKID
  AND A.STKTYPE=S.STKTYPE
  AND A.EXCHID=S.EXCHID)
WHEN MATCHED THEN
UPDATE SET
S.STKTYPENAME=A.STKTYPENAME,
S.SECU_AMOUNT=A.SECU_AMOUNT,
S.SECU_COST=A.SECU_COST,
S.SECU_TRD=A.SECU_TRD,
S.COMMISION=A.COMMISION,
S.ETF_AMT=A.ETF_AMT,
S.NEWPRICE=A.NEWPRICE,
S.SECU_MK=A.SECU_MK
WHEN NOT MATCHED THEN INSERT
(BASE_DATE, ACCTID, STKID, STKTYPE, STKTYPENAME,EXCHID, SECU_AMOUNT, SECU_COST, SECU_TRD, COMMISION, ETF_AMT,NEWPRICE, SECU_MK)
VALUES(A.BASE_DATE, A.ACCTID, A.STKID, A.STKTYPE,a.STKTYPENAME, A.EXCHID, A.SECU_AMOUNT, A.SECU_COST, A.SECU_TRD, A.COMMISION,A.ETF_AMT, A.NEWPRICE, A.SECU_MK);
COMMIT;

END LOOP;--End of position matching
END IF;



-------------------------xiaosha: Option processing start---------------------------------------
/*
 1.option fund settlement is recorded in options account, not in spot account;
2.So exercise buy/sell doesn't involve funds;

*/

IF TRD_ROW.BRIEFID IN ('005_003_054','005_004_054') THEN
---NVL(DECODE(TRD.BRIEFID, '005_003_054', 1, -1) * TRD_ROW.KNOCKQTY,
   V_MODULE_NAME:='Exercise processing';
/*   UPDATE TTRD_ACC_ETF_BALANCE T
      SET T.SECU_AMOUNT = T.SECU_AMOUNT + (CASE WHEN TRD_ROW.BRIEFID= '005_003_054' THEN  1 ELSE  -1 END) * TRD_ROW.KNOCKQTY,
          T.SECU_COST = (CASE WHEN TRD_ROW.BRIEFID= '005_004_054' THEN T.SECU_COST * (1 - TRD_ROW.KNOCKQTY/ T.SECU_AMOUNT) ELSE T.SECU_COST END),
          T.SECU_TRD = (CASE WHEN TRD_ROW.BRIEFID= '005_004_054' THEN  0 - (T.SECU_COST * TRD_ROW.KNOCKQTY/ T.SECU_AMOUNT) ELSE 0 END), --Fund settlement recorded in options account, not in spot account
         -- T.NEWPRICE = TRD_ROW.NEWPRICE,--
          --T.SECU_MK  = TRD_ROW.NEWPRICE  * (T.SECU_AMOUNT + (CASE WHEN TRD_ROW.BRIEFID= '005_003_054' THEN  1 ELSE  -1 END) * TRD_ROW.KNOCKQTY)
          \*T.D_FV,T.D_PNL  processed later****\
    WHERE T.STKID = TRD_ROW.STKID --
      AND T.ACCTID = TRD_ROW.ACCTID--
      AND T.EXCHID = TRD_ROW.EXCHID--
      AND T.BASE_DATE = V_BASE_DATE
   ;*/

  MERGE INTO TTRD_ACC_ETF_BALANCE S
  USING (SELECT V_BASE_DATE AS BASE_DATE,
                TRD_ROW.ACCTID AS ACCTID,
                TRD_ROW.STKID AS STKID,
                TRD_ROW.STKTYPE AS STKTYPE,
                TRD_ROW.STKTYPENAME AS STKTYPENAME,
                TRD_ROW.EXCHID AS EXCHID,
                TRD_ROW.BRIEFID AS BRIEFID,
                TRD_ROW.KNOCKQTY AS KNOCKQTY,
                0 AS KNOCKAMT,
                TRD_ROW.NEWPRICE AS NEWPRICE,
                TRD_ROW.KNOCKQTY * TRD_ROW.NEWPRICE AS SECU_MK
           FROM DUAL) A
  ON (A.BASE_DATE = S.BASE_DATE AND A.ACCTID = S.ACCTID AND A.STKID = S.STKID AND A.STKTYPE = S.STKTYPE AND A.EXCHID = S.EXCHID)
  WHEN MATCHED THEN
    UPDATE
       SET S.STKTYPENAME = A.STKTYPENAME,
           S.SECU_AMOUNT = NVL(S.SECU_AMOUNT, 0) +  (CASE WHEN A.BRIEFID= '005_004_054' THEN -A.KNOCKQTY ELSE A.KNOCKQTY END),
           S.SECU_COST   =  (CASE WHEN A.BRIEFID= '005_004_054' THEN S.SECU_COST * (1 - A.KNOCKQTY/ S.SECU_AMOUNT) ELSE S.SECU_COST END), --Exercise sell, cost decreases
           S.SECU_TRD = S.SECU_TRD+  (CASE WHEN A.BRIEFID= '005_004_054' THEN  -S.SECU_COST * A.KNOCKQTY/ S.SECU_AMOUNT ELSE 0 END), --Fund settlement recorded in options account, not in spot account --20220217 Correct realized P/L
           S.NEWPRICE    = A.NEWPRICE,
           S.SECU_MK    = (NVL(S.SECU_AMOUNT, 0) +  (CASE WHEN A.BRIEFID= '005_004_054' THEN -A.KNOCKQTY ELSE A.KNOCKQTY END)) * A.NEWPRICE
  WHEN NOT MATCHED THEN
    INSERT
      (BASE_DATE,
       ACCTID,
       STKID,
       STKTYPE,
       STKTYPENAME,
       EXCHID,
       SECU_AMOUNT,
       SECU_COST,
       NEWPRICE,
       SECU_MK)
    VALUES
      (A.BASE_DATE,
       A.ACCTID,
       A.STKID,
       A.STKTYPE,
       A.STKTYPENAME,
       A.EXCHID,
       A.KNOCKQTY,
       A.KNOCKAMT,
       A.NEWPRICE,
       A.SECU_MK);
  COMMIT;

COMMIT;
END IF;


/*
03
Process cash difference, cash refund
'005_001_011',\*Cash difference settlement decrease*\1
'005_002_011',\*Cash difference settlement increase*\
'005_001_012',\*Cash substitution settlement decrease*\1
'005_002_012'\*Cash substitution settlement increase*\

Process as position decrease
005_004_002 Securities transfer out
005_004_003 Non-tradable transfer out
005_004_039 Rights offering/placement transfer out (share merge)--Added 20220906
*/

IF
 /*ELSIF*/  TRD_ROW.BRIEFID IN ('005_001_011',/*Cash difference settlement decrease*/'005_001_012',/*Cash substitution settlement decrease*/'005_001_002',/*Subscription-cash substitution*/'005_004_002', /*Securities transfer out*/
 '005_004_003',/*Non-tradable transfer out*/'005_004_039'
 ) THEN
 V_MODULE_NAME:='Process cash decrease';
 DBMS_OUTPUT.PUT_LINE(TRD_ROW.STKID);

/*V_ETF_XJCE:=(CASE TRD_ROW.BRIEFID WHEN '005_001_011' THEN -1  ELSE 0 END)*NVL(TRD_ROW.KNOCKAMT,0);
V_ETF_XJFH:=(CASE TRD_ROW.BRIEFID WHEN '005_001_012' THEN -1  ELSE 0 END)*NVL(TRD_ROW.KNOCKAMT,0);
V_ETF_XJTD:=(CASE WHEN TRD_ROW.BRIEFID IN('005_001_002')  THEN -1 ELSE 0 END)*nvl(TRD_ROW.KNOCKAMT,0);*/

MERGE INTO TTRD_ACC_ETF_BALANCE S
USING(SELECT V_BASE_DATE AS BASE_DATE,
TRD_ROW.ACCTID AS ACCTID,
TRD_ROW.STKID AS STKID,
TRD_ROW.STKTYPE AS STKTYPE,
--TRD_ROW.STKTYPENAME AS STKTYPENAME,
TRD_ROW.EXCHID AS EXCHID,
sum((CASE when TRD_ROW.BRIEFID in( '005_004_002','005_004_003','005_004_039') THEN -1  ELSE 0 END)*NVL(TRD_ROW.KNOCKQTY,0) ) AS SECU_AMOUNT,--Securities transfer out\Non-tradable transfer out
sum((CASE when TRD_ROW.BRIEFID in( '005_001_011') THEN -1  ELSE 0 END)*NVL(TRD_ROW.KNOCKAMT,0)) AS ETF_XJCE,
sum((CASE when TRD_ROW.BRIEFID in( '005_001_012') THEN -1  ELSE 0 END)*NVL(TRD_ROW.KNOCKAMT,0))  AS ETF_XJFH,
sum((CASE WHEN TRD_ROW.BRIEFID IN('005_001_002')  THEN -1  ELSE 0 END)*nvl(TRD_ROW.KNOCKAMT,0)) as ETF_XJTD,
sum(TRD_ROW.commision) as commision
FROM DUAL
group by V_BASE_DATE,TRD_ROW.ACCTID,TRD_ROW.STKID,TRD_ROW.STKTYPE,TRD_ROW.EXCHID
)A
  ON( A.BASE_DATE=S.BASE_DATE
  AND A.ACCTID=S.ACCTID
  AND A.STKID=S.STKID
  AND A.STKTYPE=S.STKTYPE
  AND A.EXCHID=S.EXCHID)
WHEN MATCHED THEN
UPDATE SET
S.SECU_AMOUNT=NVL(S.SECU_AMOUNT,0)+A.SECU_AMOUNT,
S.ETF_XJCE=NVL(S.ETF_XJCE,0)+A.ETF_XJCE,
S.ETF_XJFH=NVL(S.ETF_XJFH,0)+A.ETF_XJFH,
S.ETF_XJTD=NVL(S.ETF_XJTD,0)+A.ETF_XJTD,
S.commision=NVL(S.commision,0)+A.commision
WHEN NOT MATCHED THEN INSERT
(BASE_DATE, ACCTID, STKID, STKTYPE/*,STKTYPENAME*/, EXCHID,SECU_AMOUNT,ETF_XJCE,ETF_XJFH,ETF_XJTD,commision)
VALUES(A.BASE_DATE, A.ACCTID, A.STKID, A.STKTYPE/*,a.STKTYPENAME*/, A.EXCHID,a.SECU_AMOUNT,A.ETF_XJCE,A.ETF_XJFH,A.ETF_XJTD,A.commision);
COMMIT;
END IF;

/*
04
Process today's subscription/redemption cash substitution amount, SZSE fee
01 Cash redemption needs to be added to realized, recorded separately in ETF_XJTD
02 Cash subscription needs to be deducted from realized, recorded separately in ETF_XJTD
03 Today's dividend is directly added to realized, recorded separately in IR
'005_002_025','005_002_002'--Redemption-cash substitution
'005_001_002'--Subscription-cash substitution
'005_005_001'--Dividend
SH cash difference +1 settlement
SZSE subscription/redemption fee +1 booking, cash difference +2 settlement
For trading log, SZSE subscription/redemption fee needs -1, cash difference needs -2
Process as position increase
005_003_002 Securities transfer in
005_003_005 Non-tradable transfer in
005_003_003 Bonus share transfer in
005_003_004 Rights offering/placement transfer in--Added 20220906
*/
IF TRD_ROW.BRIEFID IN('005_002_025','005_002_002'/*Redemption-cash substitution*/,'005_002_011'/*Cash difference settlement increase*/,'005_002_012'/*Cash substitution settlement increase*/
  ,'005_005_001',/*Dividend*/'005_003_003',/*Bonus share transfer in*/
  '005_003_002',/*Securities transfer in*/
  '005_003_005',/*Non-tradable transfer in*/
  '005_003_004'/*Rights offering/placement transfer in*/) THEN
   V_MODULE_NAME:='Process cash increase';
--V_ETF_XJTD:=(CASE WHEN TRD_ROW.BRIEFID IN('005_002_002','005_002_025')  THEN 1 WHEN TRD_ROW.BRIEFID IN( '005_001_002') THEN -1 ELSE 0 END) *TRD_ROW.KNOCKAMT;

MERGE INTO TTRD_ACC_ETF_BALANCE S
USING(
SELECT V_BASE_DATE AS BASE_DATE,
TRD_ROW.ACCTID AS ACCTID,
TRD_ROW.STKID AS STKID,
TRD_ROW.STKTYPE AS STKTYPE,
--TRD_ROW.STKTYPENAME AS STKTYPENAME,
TRD_ROW.EXCHID AS EXCHID,
sum((CASE when TRD_ROW.BRIEFID in( '005_003_002','005_003_005','005_003_003','005_003_004') THEN NVL(TRD_ROW.KNOCKQTY,0)  ELSE 0 END)) AS SECU_AMOUNT,--Securities transfer in/*Non-tradable transfer in*//*Bonus share transfer in*//*Rights offering/placement transfer in*/
sum((CASE when TRD_ROW.BRIEFID in( '005_002_011') THEN 1  ELSE 0 END)*NVL(TRD_ROW.KNOCKAMT,0)) AS ETF_XJCE,
sum((CASE when TRD_ROW.BRIEFID in( '005_002_012') THEN 1  ELSE 0 END)*NVL(TRD_ROW.KNOCKAMT,0))  AS ETF_XJFH,
sum((CASE WHEN TRD_ROW.BRIEFID IN('005_002_025','005_002_002')  THEN 1 ELSE 0 END)*nvl(TRD_ROW.KNOCKAMT,0)) as ETF_XJTD,
sum(TRD_ROW.COMMISION) AS COMMISION,
sum((CASE WHEN TRD_ROW.BRIEFID ='005_005_001'THEN TRD_ROW.KNOCKAMT ELSE 0 END ))AS IR--Dividend income
FROM   DUAL
group by V_BASE_DATE,TRD_ROW.ACCTID,TRD_ROW.STKID,TRD_ROW.STKTYPE,TRD_ROW.EXCHID
)A
  ON( A.BASE_DATE=S.BASE_DATE
  AND A.ACCTID=S.ACCTID
  AND A.STKID=S.STKID
  AND A.STKTYPE=S.STKTYPE
  AND A.EXCHID=S.EXCHID)
  WHEN MATCHED THEN
    UPDATE SET
    S.SECU_AMOUNT=NVL(S.SECU_AMOUNT,0)+A.SECU_AMOUNT,
    S.ETF_XJCE=NVL(S.ETF_XJCE,0)+A.ETF_XJCE,
    S.ETF_XJFH=NVL(S.ETF_XJFH,0)+A.ETF_XJFH,
    S.ETF_XJTD=NVL(S.ETF_XJTD,0)+A.ETF_XJTD,
    S.COMMISION=NVL(S.COMMISION,0)+A.COMMISION,
    S.IR=NVL(S.IR,0)+NVL(A.IR,0)
  WHEN NOT MATCHED THEN INSERT
  (BASE_DATE, ACCTID, STKID, STKTYPE,/*STKTYPENAME,*/ EXCHID,SECU_AMOUNT,ETF_XJCE,ETF_XJFH,ETF_XJTD,commision,IR)
  VALUES
  (A.BASE_DATE, A.ACCTID, A.STKID, A.STKTYPE,/*A.STKTYPENAME,*/ A.EXCHID,A.SECU_AMOUNT,A.ETF_XJCE,A.ETF_XJFH,A.ETF_XJTD,A.COMMISION,A.IR);
  COMMIT;
END IF;
/*
Process combination fee/depository fee: 005_005_069
Modified 2021-11-16

*/
IF TRD_ROW.BRIEFID IN( '005_005_069') then
  V_MODULE_NAME:='Process combination fee/depository fee';
MERGE INTO TTRD_ACC_ETF_BALANCE S
USING(
SELECT V_BASE_DATE AS BASE_DATE,
TRD_ROW.ACCTID AS ACCTID,
TRD_ROW.STKID AS STKID,
TRD_ROW.STKTYPE AS STKTYPE,
--TRD_ROW.STKTYPENAME AS STKTYPENAME,
TRD_ROW.EXCHID AS EXCHID,
sum(abs(TRD_ROW.RECKONINGAMT))AS COMMISION--Negative 0.01
FROM DUAL
group by V_BASE_DATE,TRD_ROW.ACCTID,TRD_ROW.STKID,TRD_ROW.STKTYPE,TRD_ROW.EXCHID
)A
  ON( A.BASE_DATE=S.BASE_DATE
  AND A.ACCTID=S.ACCTID
  AND A.STKID=S.STKID
  AND A.STKTYPE=S.STKTYPE
  AND A.EXCHID=S.EXCHID)
  WHEN MATCHED THEN
    UPDATE SET
    S.COMMISION=NVL(S.COMMISION,0)+A.COMMISION
  WHEN NOT MATCHED THEN INSERT
  (BASE_DATE, ACCTID, STKID, STKTYPE,/*STKTYPENAME,*/ EXCHID,commision)
  VALUES
  (A.BASE_DATE, A.ACCTID, A.STKID, A.STKTYPE,/*A.STKTYPENAME,*/ A.EXCHID,A.COMMISION);
  COMMIT;
  end if;
END LOOP;--End of trading log reading

 V_MODULE_NAME:='Write subscription/redemption list cash difference';
--Read subscription/redemption list cash difference ETF_XJCE_QD
merge into TTRD_ACC_ETF_BALANCE s
using(
select
V_BASE_DATE AS base_date,
STKTYPE,
ACCTID,
EXCHID,
NVL(e.i_code1,STKID) AS STKID,
sum(
case when BRIEFID = '005_002_003' and KNOCKQTY<MINVOLUME then PRECASHCOMPONENT /*Redemption shares less than minimum redemption shares, calculate cash difference based on minimum shares*/
else
decode(BRIEFID,'005_002_003'/*Redemption*/,1,'005_001_003'/*Subscription*/,-1 )*KNOCKQTY/MINVOLUME*PRECASHCOMPONENT
end ) as ETF_XJCE_QD
from
(
   SELECT SUBSTR(TRD.OCCURTIME, 1, 8) AS OCCURTIME, --Previous day
               TRD.ACCTID, --Fund account
               TRD.BRIEFID, --Instruction
               BF.EXTERIORDESC, --Instruction description
               TRD.EXCHID, --Market
               TRD.STKTYPE,
               TRD.STKID, --Contract code
               SUM(TRD.KNOCKQTY) AS KNOCKQTY --Execution quantity
        FROM   XIR_TRD.VG_TRADINGLOG TRD--Genius trading log table
        INNER   JOIN CTSDB.BRIEFDEFINE@CTSSP BF
ON     TRD.BRIEFID = BF.BRIEFID
        WHERE  TRD.ACCTID = '100000000515'
        AND TRD.BRIEFID  IN('005_001_003'/*Subscription*/,'005_002_003'/*Redemption*/)
        --Only take funds
        and STKTYPE in('C2','A8','A3')
        AND ((SUBSTR(TRD.OCCURTIME, 1, 8) =REPLACE(V_pr1_DATE,'-')/*Previous day*/ and TRD.exchid='0')
             or(SUBSTR(TRD.OCCURTIME, 1, 8) =REPLACE(V_pr2_DATE,'-')/*2 days ago*/ and TRD.exchid='1') )
        GROUP  BY SUBSTR(TRD.OCCURTIME, 1, 8),
                  TRD.ACCTID, --Fund account
                  TRD.BRIEFID, --Instruction
                  BF.EXTERIORDESC, --Instruction description
                  TRD.EXCHID, --Market
                  TRD.STKTYPE,
                  TRD.STKID --Contract code
                  )trd
   INNER join xir_md.tfnd_etf e on trd.STKID=e.i_code and trd.OCCURTIME=replace(e.preTRADINGDAY,'-')
   group  by ACCTID, EXCHID, STKTYPE,NVL(e.i_code1,STKID))a
 ON( A.BASE_DATE=S.BASE_DATE
  AND A.ACCTID=S.ACCTID
  AND A.STKID=S.STKID
  AND A.STKTYPE=S.STKTYPE
  AND A.EXCHID=S.EXCHID)
  WHEN MATCHED THEN
    UPDATE SET s.ETF_XJCE_QD= a.ETF_XJCE_QD
    WHEN NOT MATCHED THEN INSERT
  (BASE_DATE, ACCTID, STKID, STKTYPE,/*STKTYPENAME,*/ EXCHID,ETF_XJCE_QD)
  VALUES
  (A.BASE_DATE, A.ACCTID, A.STKID, A.STKTYPE,/*A.STKTYPENAME,*/ A.EXCHID,A.ETF_XJCE_QD);
  COMMIT;
   commit;

 V_MODULE_NAME:='Write Xinyi closing price';
 --Use Xinyi's closing price to calculate market value, Xinyi calculates ex-rights price during evening clearing
merge into TTRD_ACC_ETF_BALANCE s
using(
select a.BASE_DATE as BASE_DATE,
       a.ACCTID as ACCTID,
       decode(A.STKID,'560013','560010',A.STKID) as STKID,
       a.EXCHID as EXCHID,
       a.STKTYPE as STKTYPE,
       round(a.secu_amount * round(xy.FAIR_PRICE,4) ,2) as secu_mk,
       round(xy.FAIR_PRICE,3) as FAIR_PRICE
  from TTRD_ACC_ETF_BALANCE a
  inner join (select distinct v_base_date as hold_Date, --xiaosha changed to inner join, only adjust specified account
                             decode(TRADE_MKT_CODE, 'XSHG', '0', 'XSHE', '1') as exchid,
                             SEC_CODE,
                             FAIR_PRICE
               from xir_trd.vtrd_hd_sec_income_hold xy
              where 1 = 1
                and CALC_VIEW_TYPE = '2'
                and fund_acc = '101100000000515'
                and hold_status='01'--20220904
                and hold_Date = replace(v_base_date, '-')) xy
    on a.base_date = xy.hold_date
   and decode(a.stkid,'560013','560010',a.stkid) = xy.sec_code
   and a.exchid = xy.exchid
   where secu_amount<>0
   and a.base_date=v_base_date)a
 ON( A.BASE_DATE=S.BASE_DATE
  AND A.ACCTID=S.ACCTID
  AND decode(A.STKID,'560013','560010',A.STKID)=decode(S.STKID,'560013','560010',S.STKID)
  AND A.STKTYPE=S.STKTYPE
  AND A.EXCHID=S.EXCHID)
  WHEN MATCHED THEN
    UPDATE SET s.secu_mk= a.secu_mk, s.fair_price=a.fair_price;
   commit;


 V_MODULE_NAME:='Process daily total P/L';
/*
05
After trading log processing
Update daily total P/L*/
UPDATE TTRD_ACC_ETF_BALANCE S
SET    (D_FV, D_PNL) =
       (SELECT D_FV,
               D_PNL
        FROM   (SELECT BASE_DATE,
                       ACCTID,
                       STKID,
                       EXCHID,
                       STKTYPE,
                       /*Daily floating P/L*/
                       (NVL(SECU_MK, 0) - NVL(SECU_COST, 0)) -
                       LAG((NVL(SECU_MK, 0) - NVL(SECU_COST, 0)), 1, 0) OVER(PARTITION BY ACCTID, STKID, EXCHID, STKTYPE ORDER BY BASE_DATE) AS D_FV,
                       /*Daily total P/L*/
                       NVL(SECU_TRD, 0) + NVL(ETF_XJCE_QD, 0)/*Cash difference from subscription/redemption list*/ + NVL(ETF_XJFH, 0) -
                       NVL(COMMISION, 0) + NVL(ETF_XJTD, 0) + NVL(ETF_AMT, 0) +nvl(IR,0)+
                       NVL((NVL(SECU_MK, 0) - NVL(SECU_COST, 0)) -
                       LAG((NVL(SECU_MK, 0) - NVL(SECU_COST, 0)), 1, 0) OVER(PARTITION BY ACCTID, STKID, EXCHID, STKTYPE ORDER BY BASE_DATE),0) AS D_PNL
                FROM   TTRD_ACC_ETF_BALANCE B
                WHERE  1=1--SECU_COST + SECU_AMOUNT <> 0
                AND    BASE_DATE IN (V_BASE_DATE ,V_PR1_DATE/*Previous trading day*/)) B
        WHERE  B.BASE_DATE = S.BASE_DATE
        AND    B.ACCTID = S.ACCTID
        AND    B.STKID = S.STKID
        AND    B.EXCHID = S.EXCHID
        AND    B.STKTYPE = S.STKTYPE
        AND    B.BASE_DATE = V_BASE_DATE)
WHERE  BASE_DATE = V_BASE_DATE;
  COMMIT;

V_UPDATE_CNT:=  SQL%ROWCOUNT;
 --Console output
    P_RESULT      := 1;
    P_RESULT_INFO := 'ETF P/L calculation executed successfully' || 'Start date:' || P_BEG_DATE || 'End date:' ||
                     P_END_DATE;
    --Write execution success log:
    INSERT INTO TTRD_PROC_RUN_LOG --20220325 Modified log writing
      (PROC_NAME, BASE_DATE, LOG_STATUS, LOG_DESC, UPDATE_CNT)
    VALUES
      ('PKG_RPT_DAILY_BALANCE_ETF',
       V_base_date,
       P_RESULT,
       P_RESULT_INFO,
       V_UPDATE_CNT);
    COMMIT;

  END LOOP;--End of date loop
  EXCEPTION
    WHEN OTHERS THEN
      --Console output
      P_RESULT      := 0;
      P_RESULT_INFO := 'ETF P/L calculation failed'|| 'Date:' || V_base_date || 'Location:' ||
                       V_MODULE_NAME ||SQLCODE || '---' || SQLERRM;
      ROLLBACK;
    --Record execution failure log:
     INSERT INTO TTRD_PROC_RUN_LOG
      (PROC_NAME, BASE_DATE, LOG_STATUS, LOG_DESC, UPDATE_CNT)
    VALUES
      ('PKG_RPT_DAILY_BALANCE_ETF',
       V_base_date,
       P_RESULT,
       P_RESULT_INFO,
       V_UPDATE_CNT);
      COMMIT;

END P_RPT_DAILY_BALANCE_ETF;

END PKG_RPT_DAILY_BALANCE_ETF;
/
