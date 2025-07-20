CREATE OR REPLACE PACKAGE PKG_RPT_DAILY_BALANCE_FUT IS
/*
DATE: 2021-04-30
Purpose: Used to execute P_RPT_DAILY_BALANCE_FUT_SYB (Gengsheng logic) and P_RPT_DAILY_BALANCE_FUT_DETAIL (FIFO logic)
*/

PROCEDURE P_RPT_DAILY_BALANCE_FUT(  P_BEG_DATE    IN VARCHAR2, --Start date
                                    P_END_DATE    IN VARCHAR2, --End date
                                    P_RESULT      OUT VARCHAR2, --Execution result
                                    P_RESULT_INFO OUT VARCHAR2 --Execution details
                                   );

/*
DATE: 2021-03-26
Target table: XIR_TRD.TTRD_ACC_FUT_SYB
Purpose: Calculate futures profit/loss, logic references original Gengsheng code
*/
PROCEDURE P_RPT_DAILY_BALANCE_FUT_SYB(P_BEG_DATE    IN VARCHAR2,
                                      P_END_DATE    IN VARCHAR2,
                                      P_RESULT      OUT VARCHAR2, --Execution result
                                      P_RESULT_INFO OUT VARCHAR2 --Execution details
                                  );
/*
DATE: 2021-04-30
Target tables: XIR_TRD.TTRD_ACC_FUT_TRDDETAIL--Transaction detail table
               XIR_TRD.TTRD_ACC_FUT_BALANCE--Position summary table																	
Purpose: Calculate futures profit/loss, take Genius trading logs and calculate using FIFO method			
*/													
PROCEDURE P_RPT_DAILY_BALANCE_FUT_DETAIL(P_BEG_DATE    IN VARCHAR2,
                                         P_END_DATE    IN VARCHAR2,
                                         --P_I_CODE      IN VARCHAR2, --Test single contract code
                                         P_RESULT      OUT VARCHAR2, --Execution result
                                         P_RESULT_INFO OUT VARCHAR2 --Execution details
                                         );																	
END PKG_RPT_DAILY_BALANCE_FUT;
/
CREATE OR REPLACE PACKAGE BODY PKG_RPT_DAILY_BALANCE_FUT IS
/*
   Date: 2021-08-24
   Author: Huang Xiaosha
  Note: 1) Added log writing;
  
   Date: 2022-08-01
   Author: Tian Fangning
   Note: Added IM for stock index options
*/
  PROCEDURE P_RPT_DAILY_BALANCE_FUT( P_BEG_DATE    IN VARCHAR2, --Start date
                                              P_END_DATE    IN VARCHAR2, --End date
                                              P_RESULT      OUT VARCHAR2, --Execution result
                                              P_RESULT_INFO OUT VARCHAR2 --Execution details
/*
CREATOR: WB_CHENGDH
DATE: 2021-04-30
Purpose: Used to execute P_RPT_DAILY_BALANCE_FUT_SYB (Gengsheng logic) and P_RPT_DAILY_BALANCE_FUT_DETAIL (FIFO logic)
*/                                           ) IS
 V_CNT INTEGER:=0;
 PROC_NAME VARCHAR2(100):='PKG_RPT_DAILY_BALANCE_FUT.P_RPT_DAILY_BALANCE_FUT';--Procedure name 
BEGIN
  --Take Genius trading logs and calculate profit/loss using FIFO method
  PKG_RPT_DAILY_BALANCE_FUT.P_RPT_DAILY_BALANCE_FUT_DETAIL(P_BEG_DATE,P_END_DATE,P_RESULT,P_RESULT_INFO);
  --Calculate profit/loss referencing original Gengsheng code
  PKG_RPT_DAILY_BALANCE_FUT.P_RPT_DAILY_BALANCE_FUT_SYB(P_BEG_DATE,P_END_DATE,P_RESULT,P_RESULT_INFO);
   --Success output
    P_RESULT      := 1;
    P_RESULT_INFO := 'Futures P/L execution details' || 'Start date:' || P_BEG_DATE || 'End date:' ||
                     P_END_DATE || '---' || 'Execution successful';
  EXCEPTION
    WHEN OTHERS THEN
    --Failure output
      P_RESULT      := 0;
      P_RESULT_INFO := 'Futures P/L execution details'|| 'Start date:' || P_BEG_DATE || 'End date:' ||
                      P_END_DATE || '---' || 'Execution failed'|| ':' ||  SQLERRM;


      ROLLBACK;

  END P_RPT_DAILY_BALANCE_FUT;


  PROCEDURE P_RPT_DAILY_BALANCE_FUT_SYB(P_BEG_DATE    IN VARCHAR2, --Start date
                                        P_END_DATE    IN VARCHAR2, --End date
                                        P_RESULT      OUT VARCHAR2, --Execution result
                                        P_RESULT_INFO OUT VARCHAR2 --Execution details
                                    ) IS
/*
DATE: 2021-03-26
Target table: XIR_TRD.TTRD_ACC_FUT_SYB
Purpose: Calculate futures profit/loss, logic references original Gengsheng code
*/
    V_COMMISION    NUMBER(38, 8); --Commission
    V_LAST_TRDDATE VARCHAR2(10); --Previous trading day
    V_SQL          CLOB; --SQL code
    --Log output
    PROC_NAME   VARCHAR2(100); --Procedure name
    S_MODULE_NAME VARCHAR2(20); --Module name
    V_CNT   NUMBER(38, 8); --Update record count
    S_LOG_DESC    VARCHAR2(1000); --Log details
    --Target table: XIR_TRD.TTRD_ACC_FUT_SYB
    --Table fields: Position, commission, declaration fee, margin, position P/L, closed position P/L, mark-to-market P/L
    --Log table: XIR_TRD.TTRD_PROC_RUN_LOG
  BEGIN
    PROC_NAME := 'PKG_RPT_DAILY_BALANCE_FUT.P_RPT_DAILY_BALANCE_FUT_SYB';
    --Delete data
    DELETE FROM XIR_TRD.TTRD_ACC_FUT_SYB WHERE BASE_DATE BETWEEN P_BEG_DATE AND P_END_DATE;
    COMMIT;
    --Get trading days, starting from end of 2019
    FOR CR IN (SELECT CAL_DAY AS BASE_DATE
                 FROM XIR_MD.TCALENDAR_DATES T
                WHERE T.CAL_CODE = 'CHINA_EX' --Exchange calendar
                  AND T.CAL_FLAG = 1 --Trading day
                  AND T.CAL_DAY >= '2019-12-30'
                  AND T.CAL_DAY BETWEEN P_BEG_DATE AND P_END_DATE) LOOP
           
      --1. Write Genius position quantity
      S_MODULE_NAME := 'MERGE:POSITIONQTY';
      V_SQL         := 'MERGE INTO XIR_TRD.TTRD_ACC_FUT_SYB A
USING (SELECT ''' || CR.BASE_DATE ||
                       ''' AS BASE_DATE,
              BRANCHID,
              ACCTID,
              STKID AS I_CODE,
              TRANSLATE(BSFLAG, ''BS'', ''LS'') AS LS,
              OFFERREGID,--Speculation/hedging account
              ZT.LX AS OFFERREGNAME,--Speculation/hedging name
              CURRENTPOSITIONQTY AS POSITIONQTY,--Position quantity
              CLOSEPNL + REALTIMEPNL AS DSYK    --Mark-to-market P/L
       FROM CTSDB.FUTUREPOSITION' ||
                       SUBSTR(CR.BASE_DATE, 1, 4) ||
                       '@CTSSP T --Genius-Futures position table 2020
       INNER  JOIN (SELECT SUBSTR(JYBM, -8) AS JYBM,
                          LX
                   FROM   XIR_TRD.TTRD_SRC_TRADE_CODE_ZT --Settlement statement-trading code, determine speculation/hedging flag
                   WHERE  1 = 1
                   AND    ACCID IN (''28277777'', ''28288888'')
                   AND    BEG_DATE = ''2020-01-20'') ZT --------To be adjusted
       ON     T.OFFERREGID = ZT.JYBM
       WHERE  SUBSTR(T.OCCURTIME, 1, 8) = REPLACE(''' ||
                       CR.BASE_DATE || ''', ''-'')
       AND    F_PRODUCTID IN (''IF'', ''IC'', ''IM'',''IH'',''IM'')/*Stock index futures*/) B
ON (A.BASE_DATE = B.BASE_DATE AND A.BRANCHID = B.BRANCHID AND A.ACCTID = B.ACCTID AND A.I_CODE = B.I_CODE AND A.LS = B.LS AND A.OFFERREGID = B.OFFERREGID AND A.OFFERREGNAME = B.OFFERREGNAME)
WHEN MATCHED THEN
UPDATE
SET    A.POSITIONQTY = B.POSITIONQTY,
       A.DSYK        = B.DSYK
WHEN NOT MATCHED THEN
INSERT
(BASE_DATE,
 BRANCHID,
 ACCTID,
 I_CODE,
 LS,
 OFFERREGID,
 OFFERREGNAME,
 FDYK,
 PCYK,
 MARGINUSED,
 COMMISION,
 SBF,
 POSITIONQTY,
 DSYK,
 CREATE_TIME)
VALUES
(B.BASE_DATE,
 B.BRANCHID,
 B.ACCTID,
 B.I_CODE,
 B.LS,
 B.OFFERREGID,
 B.OFFERREGNAME,
 0,
 0,
 0,
 0,
 0,
 B.POSITIONQTY,
 B.DSYK,
 SYSDATE)';
      --DBMS_OUTPUT.PUT_LINE(V_SQL);
      EXECUTE IMMEDIATE V_SQL;
      --Records updated this time:
      --V_CNT := SQL%ROWCOUNT;
      V_CNT := V_CNT+ SQL%ROWCOUNT; --2021-08-24 Record update data volume
      COMMIT;
      --2. Write declaration fee
      S_MODULE_NAME := 'MERGE:SBF';
      V_SQL         := '
MERGE INTO XIR_TRD.TTRD_ACC_FUT_SYB A
USING (SELECT''' || CR.BASE_DATE ||
                       ''' AS BASE_DATE,
              BRANCHID,
              ACCTID,
              I_CODE,
              LS,
              OFFERREGID,
              COUNT(1) AS SBF--Declaration fee
       FROM   (SELECT SUBSTR(OCCURTIME, 1, 8) AS OCCURTIME,
                      BRANCHID,
                      ACCTID,
                      STKID AS I_CODE,
                      CASE F_OFFSETFLAG || BSFLAG
                      WHEN ''OPENB'' THEN
                       ''L''
                      WHEN ''CLOSES'' THEN
                       ''L''
                      WHEN ''OPENS'' THEN
                       ''S''
                      WHEN ''CLOSEB'' THEN
                       ''S''
                      END AS LS,
                      F_HEDGEFLAG,
                      OFFERREGID
               FROM   CTSDB.FUTUREOPENORDER' ||
                       SUBSTR(CR.BASE_DATE, 1, 4) ||
                       '@CTSSP F
               WHERE  SUBSTR(F.OCCURTIME, 1, 8) =REPLACE(''' ||
                       CR.BASE_DATE || ''', ''-'')
               AND    F_PRODUCTID IN (''IC'', ''IF'', ''IH'',''IM'')
               AND    EXCHID = ''F'' --CFFEX
               AND    VALIDFLAG = 0) F
       GROUP  BY TO_CHAR(TO_DATE(SUBSTR(OCCURTIME, 1, 8), ''YYYYMMDD''),''YYYY-MM-DD''),
                 BRANCHID,
                 ACCTID,
                 I_CODE,
                 LS,
                 F_HEDGEFLAG,
                 OFFERREGID) B
ON (A.BASE_DATE = B.BASE_DATE AND A.BRANCHID = B.BRANCHID AND A.ACCTID = B.ACCTID AND A.I_CODE = B.I_CODE AND A.LS = B.LS AND A.OFFERREGID = B.OFFERREGID)
WHEN MATCHED THEN
UPDATE SET A.SBF = B.SBF
WHEN NOT MATCHED THEN
INSERT
(BASE_DATE,
 BRANCHID,
 ACCTID,
 I_CODE,
 LS,
 OFFERREGID,
 SBF,
 CREATE_TIME)
VALUES
(B.BASE_DATE,
 B.BRANCHID,
 B.ACCTID,
 B.I_CODE,
 B.LS,
 B.OFFERREGID,
 B.SBF,
 SYSDATE)';
      --DBMS_OUTPUT.PUT_LINE(V_SQL);
      EXECUTE IMMEDIATE V_SQL;
      COMMIT;
      --3. Write commission
      S_MODULE_NAME := 'MERGE:COMMISION';
      V_SQL         := '
MERGE INTO XIR_TRD.TTRD_ACC_FUT_SYB A
USING (SELECT''' || CR.BASE_DATE ||
                       '''AS BASE_DATE,
              BRANCHID,
              ACCTID,
              STKID AS I_CODE,
              CASE F_OFFSETFLAG || BSFLAG
              WHEN ''OPENB'' THEN
               ''L''
              WHEN ''CLOSES'' THEN
               ''L''
              WHEN ''OPENS'' THEN
               ''S''
              WHEN ''CLOSEB'' THEN
               ''S''
              END AS LS,
              SUM(NVL(CUSTCOMMISION, 0)) AS COMMISION,--Commission
              OFFERREGID
       FROM   (SELECT OCCURTIME,
                      BRANCHID,
                      ACCTID,
                      STKID,
                      F_OFFSETFLAG,
                      BSFLAG,
                      CUSTCOMMISION,
                      F_HEDGEFLAG,
                      OFFERREGID,
                      F_PRODUCTID
               FROM   CTSDB.FUTURETRADINGLOG' ||
                       SUBSTR(CR.BASE_DATE, 1, 4) ||
                       '@CTSSP
               WHERE  SUBSTR(OCCURTIME, 1, 8) = REPLACE(''' ||
                       CR.BASE_DATE || ''', ''-'')
               AND    F_PRODUCTID IN (''IF'', ''IC'', ''IH'',''IM'')
               AND    CUSTCOMMISION <> 0) F
       GROUP  BY SUBSTR(OCCURTIME, 1, 8),
                 BRANCHID,
                 ACCTID,
                 STKID,
                 CASE F_OFFSETFLAG || BSFLAG
                 WHEN ''OPENB'' THEN
                  ''L''
                 WHEN ''CLOSES'' THEN
                  ''L''
                 WHEN ''OPENS'' THEN
                  ''S''
                 WHEN ''CLOSEB'' THEN
                  ''S''
                 END,
                 F_HEDGEFLAG,
                 OFFERREGID) B
ON (A.BASE_DATE = B.BASE_DATE AND A.BRANCHID = B.BRANCHID AND A.ACCTID = B.ACCTID AND A.I_CODE = B.I_CODE AND A.LS = B.LS AND A.OFFERREGID = B.OFFERREGID)
WHEN MATCHED THEN
UPDATE SET A.COMMISION = B.COMMISION
WHEN NOT MATCHED THEN
INSERT
(BASE_DATE,
 BRANCHID,
 ACCTID,
 I_CODE,
 LS,
 OFFERREGID,
 COMMISION,
 CREATE_TIME)
VALUES
(B.BASE_DATE,
 B.BRANCHID,
 B.ACCTID,
 B.I_CODE,
 B.LS,
 B.OFFERREGID,
 B.COMMISION,
 SYSDATE)';
      --DBMS_OUTPUT.PUT_LINE(V_SQL);
      EXECUTE IMMEDIATE V_SQL;
      COMMIT;
      --4. Write floating P/L, margin
      S_MODULE_NAME := 'MERGE:FDYK';
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_SYB A
      USING (SELECT FUT.BASE_DATE,
                    FUT.BRANCHID,
                    FUT.ACCTID,
                    FUT.I_CODE,
                    FUT.LS,
                    FUT.OFFERREGID,
                    FUT.OFFERREGNAME,
                    FUT.POSITIONQTY,
                    ROUND(NVL(FUT.POSITIONQTY, 0) / NULLIF(JSD.AMOUNT, 0) *
                          NVL(JSD.FDYK, 0),
                          4) AS FDYK, --Floating P/L: Genius position/settlement position * settlement floating P/L
                    ROUND(NVL(FUT.POSITIONQTY, 0) / NULLIF(JSD.AMOUNT, 0) *
                          NVL(JSD.BZJ, 0),
                          4) AS MARGINUSED --Margin: Genius position/settlement position * settlement margin
               FROM (SELECT H.*, ZT.JYBM AS OFFERREGID
                       FROM (SELECT ACCID,
                                    BEG_DATE AS BASE_DATE,
                                    HYDM AS I_CODE,
                                    LS,
                                    TT AS OFFERREGNAME,
                                    SUM(NVL(AMOUNT, 0)) AS AMOUNT, --Settlement position
                                    SUM(NVL(LYBZJ, 0)) AS BZJ, --Settlement margin
                                    SUM(NVL(FDYK, 0)) AS FDYK --Settlement floating P/L
                               FROM XIR_TRD.TTRD_SRC_POSITION_DETAIL_ZT --Settlement statement
                                    UNPIVOT((AMOUNT) FOR LS IN((BUY_COUNT) AS 'L',
                                                               (SELL_COUNT) AS 'S'))
                              WHERE BEG_DATE = CR.BASE_DATE
                                AND FILENAME LIKE '%Mark-to-market%'
                                AND AMOUNT <> 0
                                AND SUBSTR(HYDM, 1, 2) IN ('IF', 'IC', 'IH','IM')
                              GROUP BY ACCID, BEG_DATE, HYDM, LS, TT) H
                      INNER JOIN (SELECT ACCID, SUBSTR(JYBM, -8) AS JYBM, LX
                                   FROM TTRD_SRC_TRADE_CODE_ZT --Settlement statement-trading code, determine speculation/hedging flag
                                  WHERE BEG_DATE = '2020-01-20') ZT
                         ON H.ACCID = ZT.ACCID
                        AND H.OFFERREGNAME = ZT.LX) JSD
               LEFT JOIN XIR_TRD.TTRD_ACC_FUT_SYB FUT
                 ON FUT.I_CODE = JSD.I_CODE
                AND FUT.LS = JSD.LS
                AND FUT.BASE_DATE = JSD.BASE_DATE
                AND FUT.OFFERREGID = JSD.OFFERREGID
                AND FUT.OFFERREGNAME = JSD.OFFERREGNAME) B
      ON (A.BASE_DATE = B.BASE_DATE AND A.BRANCHID = B.BRANCHID AND A.ACCTID = B.ACCTID AND A.I_CODE = B.I_CODE AND A.LS = B.LS AND A.OFFERREGID = B.OFFERREGID AND A.OFFERREGNAME = B.OFFERREGNAME)
      WHEN MATCHED THEN
        UPDATE SET A.FDYK = B.FDYK, A.MARGINUSED = B.MARGINUSED
      WHEN NOT MATCHED THEN
        INSERT
          (BASE_DATE,
           BRANCHID,
           ACCTID,
           I_CODE,
           LS,
           OFFERREGID,
           OFFERREGNAME,
           POSITIONQTY,
           FDYK,
           MARGINUSED,
           CREATE_TIME)
        VALUES
          (B.BASE_DATE,
           B.BRANCHID,
           B.ACCTID,
           B.I_CODE,
           B.LS,
           B.OFFERREGID,
           B.OFFERREGNAME,
           B.POSITIONQTY,
           B.FDYK,
           B.MARGINUSED,
           SYSDATE);
      COMMIT;
      --Get previous trading day
      SELECT MAX(CAL_DAY)
        INTO V_LAST_TRDDATE
        FROM XIR_MD.TCALENDAR_DATES T
       WHERE T.CAL_CODE = 'CHINA_EX' --Exchange
         AND T.CAL_FLAG = 1 --Trading day
         AND CAL_DAY < CR.BASE_DATE;
      ---Special handling: If department commission=0 for the day, then closed position P/L=0, floating P/L=mark-to-market P/L + previous floating P/L
      --Temporarily only check 80002 quantitative department
      DECLARE
        CURSOR C_LASTDATA IS
          SELECT *
            FROM XIR_TRD.TTRD_ACC_FUT_SYB
           WHERE BASE_DATE = V_LAST_TRDDATE
             AND BRANCHID = '800002';
        C_ROW C_LASTDATA%ROWTYPE;
      BEGIN
        SELECT SUM(NVL(COMMISION, 0))
          INTO V_COMMISION
          FROM XIR_TRD.TTRD_ACC_FUT_SYB
         WHERE BASE_DATE = CR.BASE_DATE
           AND BRANCHID = '800002';
        S_MODULE_NAME := 'UPDATE:FDYK';
        --DBMS_OUTPUT.PUT_LINE( '----------Commission-------'||V_COMMISION);-----Debug
        FOR C_ROW IN C_LASTDATA LOOP
          IF V_COMMISION = 0 THEN
            UPDATE XIR_TRD.TTRD_ACC_FUT_SYB A
               SET FDYK = DSYK + C_ROW.FDYK
            --,PCYK=0
             WHERE BASE_DATE = CR.BASE_DATE
               AND C_ROW.ACCTID = A.ACCTID
               AND C_ROW.I_CODE = A.I_CODE
               AND C_ROW.LS = A.LS
               AND C_ROW.BRANCHID = A.BRANCHID
               AND C_ROW.OFFERREGID = A.OFFERREGID;
          END IF;
        END LOOP;
      END;
      COMMIT;
      --5. Write closed position P/L
      S_MODULE_NAME := 'MERGE:PCYK';
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_SYB A
      USING (SELECT *
               FROM (SELECT BASE_DATE,
                            BRANCHID,
                            ACCTID,
                            I_CODE,
                            LS,
                            OFFERREGID,
                            --Check if quantitative department commission=0
                            CASE
                              WHEN BRANCHID = '800002' AND
                                   SUM(NVL(COMMISION, 0))
                               OVER(PARTITION BY BASE_DATE,
                                        BRANCHID ORDER BY BASE_DATE) = 0 THEN
                               0
                              ELSE
                               NVL(DSYK, 0) -
                               (NVL(FDYK, 0) - LAG(NVL(FDYK, 0), 1, 0)
                                OVER(PARTITION BY BRANCHID,
                                     ACCTID,
                                     I_CODE,
                                     LS,
                                     OFFERREGID ORDER BY BASE_DATE))
                            END AS PCYK --Daily closed position P/L=mark-to-market P/L-(today's floating P/L - yesterday's floating P/L)
                       FROM XIR_TRD.TTRD_ACC_FUT_SYB
                      WHERE BASE_DATE IN (V_LAST_TRDDATE, CR.BASE_DATE))
              WHERE BASE_DATE = CR.BASE_DATE) B
      ON (A.BASE_DATE = B.BASE_DATE AND A.BRANCHID = B.BRANCHID AND A.ACCTID = B.ACCTID AND A.I_CODE = B.I_CODE AND A.LS = B.LS AND A.OFFERREGID = B.OFFERREGID)
      WHEN MATCHED THEN
        UPDATE SET A.PCYK = B.PCYK
      WHEN NOT MATCHED THEN
        INSERT
          (BASE_DATE,
           BRANCHID,
           ACCTID,
           I_CODE,
           LS,
           OFFERREGID,
           PCYK,
           CREATE_TIME)
        VALUES
          (B.BASE_DATE,
           B.BRANCHID,
           B.ACCTID,
           B.I_CODE,
           B.LS,
           B.OFFERREGID,
           B.PCYK,
           SYSDATE);
      COMMIT;
/*      --Write execution success log:
      S_LOG_DESC := 'Execution successful';
      INSERT INTO XIR_TRD.TTRD_PROC_RUN_LOG
        (LOGID,BASE_DATE, PROC_NAME, LOG_STATUS, LOG_DESC, UPDATE_CNT, RUN_TIME)
      VALUES
        (NULL,CR.BASE_DATE, PROC_NAME, '1', S_LOG_DESC, V_CNT, SYSDATE);
      COMMIT;*/
    END LOOP;
    --Console output
    P_RESULT      := 1;
    P_RESULT_INFO := 'Stock index futures P/L execution details' || 'Start date:' || P_BEG_DATE || 'End date:' ||
                     P_END_DATE || '---' || 'Execution successful';
    --Write execution success log:
    INSERT INTO TTRD_PROC_RUN_LOG
      (PROC_NAME, BASE_DATE, LOG_STATUS, LOG_DESC, UPDATE_CNT)
    VALUES
      (PROC_NAME,
       P_BEG_DATE,
       P_RESULT,
       P_RESULT_INFO,
       V_CNT);  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      --Console output
      P_RESULT      := 0;
      P_RESULT_INFO := 'Stock index futures P/L execution details' || 'Start date:' || P_BEG_DATE || 'End date:' ||
                       P_END_DATE || 'Execution failed:' || SQLCODE || '---' || SQLERRM;
      ROLLBACK;
    --Record execution failure log:
     INSERT INTO TTRD_PROC_RUN_LOG
      (PROC_NAME, BASE_DATE, LOG_STATUS, LOG_DESC, UPDATE_CNT)
    VALUES
      (PROC_NAME,
       P_BEG_DATE,
       P_RESULT,
       P_RESULT_INFO,
       V_CNT);  
      COMMIT;
  END P_RPT_DAILY_BALANCE_FUT_SYB;

-------------Futures P/L: FIFO
  PROCEDURE P_RPT_DAILY_BALANCE_FUT_DETAIL(P_BEG_DATE    IN VARCHAR2, --Start date
                                           P_END_DATE    IN VARCHAR2, --End date
                                           --P_I_CODE      IN VARCHAR2, --Test single contract code
                                           P_RESULT      OUT VARCHAR2, --Execution result
                                           P_RESULT_INFO OUT VARCHAR2 --Execution details
                                           ) IS
    /*
    DATE: 2021-04-30
    Source tables: XIR_TRD.VG_FUTURETRADINGLOG--Genius futures trading log table
                   XIR_TRD.VG_FUTUREINFO--Genius futures market data table
                   XIR_TRD.VG_FUTUREOPENORDER--Genius futures order table
    Temp table: XIR_TRD.TTRD_ACC_FUT_TRADINLOG_DAY Daily trading log temp table
    Target tables: XIR_TRD.TTRD_ACC_FUT_TRDDETAIL--Transaction detail table
                   XIR_TRD.TTRD_ACC_FUT_BALANCE--Position summary table
    Log table: XIR_TRD.TTRD_PROC_RUN_LOG--Procedure execution log table
    Process:
        1.Read daily trading data into temp table
        2.Write previous day's open positions to today's position details
        3.Process trading logs: Traverse trading logs in execution number order, 
           open positions are directly written to position detail table, 
           close positions traverse position detail table (each trading log traverses once), 
           calculate latest position average price, take the earliest open position record for closing (per trade, per day)
        4.After daily open/close processing, update position average price, write settlement statement close position info, record differences
        5.Summarize detail table P/L by Genius account, contract to summary table
        6.Calculate monthly, yearly cumulative numbers in summary table
    */
    ------------------------------------Variable declaration

     PROC_NAME VARCHAR2(100):='PKG_RPT_DAILY_BALANCE_FUT.P_RPT_DAILY_BALANCE_FUT_DETAIL';--Procedure name 
    V_LAST_TRDDATE VARCHAR2(10); --Variable 1: Previous trading day
    V_DATE VARCHAR2(10);        --Variable 2: Current execution date
    CURSOR TRD_CURSOR --Variable 3: Trading log cursor: open+close data
    IS
      SELECT TO_CHAR(TO_DATE(SUBSTR(T.KNOCKTIME, 1, 8), 'YYYYMMDD'),'YYYY-MM-DD') AS BASE_DATE,
             T.BRANCHID,--Department number
             T.OFFERREGID,--Speculation/hedging code
             T.ACCTID,--Fund account
             T.BSFLAG,--Long/short flag
             T.F_OFFSETFLAG,--Open/close flag
             T.BRIEFID,--Operation instruction
             T.KNOCKTIME,--Clearing time
             T.STKID,--Contract code
             T.EXCHID,--Trading market
             T.F_PRODUCTID,--Product code
             F.CONTRACTTIMES,--Contract multiplier
             F.PRESETTLEMENTPRICE,--Previous settlement price
             F.SETTLEMENTPRICE,--Today's settlement price
             T.KNOCKQTY,--Execution quantity
             T.KNOCKPRICE,--Execution price
             T.KNOCKAMT,--Execution amount
             T.KNOCKCODE,--Execution number
             ROW_NUMBER() OVER(PARTITION BY SUBSTR(T.KNOCKTIME, 1, 8),T.KNOCKCODE ORDER BY T.KNOCKCODE)AS KCCODE_RNK,--Auxiliary column for sorting
             T.CUSTCOMMISION --Commission
        FROM XIR_TRD.TTRD_ACC_FUT_TRADINLOG_DAY T--Log temp table
        LEFT JOIN (SELECT SUBSTR(F.OCCURTIME, 1, 8) AS OCCURTIME,
                          F.STKID,
                          F.EXCHID,
                          F.F_PRODUCTID,
                          F.CONTRACTTIMES,
                          F.PRESETTLEMENTPRICE,
                          F.SETTLEMENTPRICE
                     FROM XIR_TRD.VG_FUTUREINFO F--Futures market data table
                    WHERE (F.EXCHID IN ('N', 'S', 'Z', 'D') --Commodity futures, determined by market
                          OR F.F_PRODUCTID IN ('IC', 'IF', 'IM','IH', 'T', 'TS', 'TF')) --Stock index futures, treasury futures, determined by product ID
                    AND SUBSTR(F.OCCURTIME, 1, 8) = REPLACE(V_DATE, '-')) F
         ON F.EXCHID = T.EXCHID--Market
         AND F.F_PRODUCTID = T.F_PRODUCTID--Product code
         AND F.STKID = T.STKID--Contract code
         AND F.OCCURTIME = SUBSTR(T.KNOCKTIME, 1, 8)--Date
       WHERE SUBSTR(T.KNOCKTIME, 1, 8) = REPLACE(V_DATE, '-')
       ORDER BY T.SERIALNUM,
                T.KNOCKTIME,
                T.KNOCKCODE;

    CURSOR POS_CURSOR --Variable 4: Position data cursor
    (V_STKID      VARCHAR2,
     V_BSFLAG     VARCHAR2,
     V_BRIEFID    VARCHAR2,
     V_ACCTID     VARCHAR2,
     V_BRANCHID   VARCHAR2,
     V_OFFERREGID VARCHAR2)
    IS
      SELECT *
        FROM (SELECT BASE_DATE,
                     KCTIME,
                     STKID,
                     RNK,--
                     KCSL,
                     AVGPRICE,--Moving average unit price
                     ROUND(SUM(KCAVGPRICE) OVER(PARTITION BY BASE_DATE, STKID, ACCTID, BRANCHID, OFFERREGID, KCBSFLAG),8) AS KCAVGPRICE, --Position weighted average price
                     KCJG,
                     KCAMT,
                     KCBSFLAG,
                     KCF_OFFSETFLAG,
                     ACCTID,
                     KCCODE,
                     EXCHID,
                     COMMISION,
                     BRANCHID,
                     OFFERREGID,
                     UNCLOSEQTY,--Unclosed quantity
                     CONTRACTTIMES,
                     PRESETTLEMENTPRICE,
                     SETTLEMENTPRICE,
                     F_PRODUCTID,
                     COST_D,
                     FDSY_D,--Daily floating P/L
                     KCCODE_RNK
                FROM (SELECT T.BASE_DATE,
                             T.KCTIME,
                             T.STKID,
                             ROW_NUMBER() OVER( /*PARTITION BY STKID, KCBSFLAG, KCF_OFFSETFLAG, ACCTID, BRANCHID, OFFERREGID*/ /*,KNOCKCODE*/ ORDER BY KCTIME, KCCODE) AS RNK,
                             T.KCSL,
                             AVGPRICE,
                             T.KCJG * RATIO_TO_REPORT(UNCLOSEQTY) OVER(PARTITION BY T.BASE_DATE, T.STKID, T.ACCTID,  T.BRANCHID, T.OFFERREGID, T.KCBSFLAG) AS KCAVGPRICE, --Calculate position average price
                             T.KCJG,
                             T.KCAMT,
                             T.KCBSFLAG,
                             T.KCF_OFFSETFLAG,
                             T.ACCTID,
                             T.KCCODE,
                             T.EXCHID,
                             T.COMMISION,
                             T.BRANCHID,
                             T.OFFERREGID,
                             T.UNCLOSEQTY,
                             T.CONTRACTTIMES,
                             T.PRESETTLEMENTPRICE,
                             T.SETTLEMENTPRICE,
                             T.F_PRODUCTID,
                             T.COST_D,
                             T.FDSY_D,
                             KCCODE_RNK
                        FROM XIR_TRD.TTRD_ACC_FUT_TRDDETAIL T
                       WHERE SUBSTR(T.KCTIME,1,8)=CASE WHEN V_BRIEFID IN('205_002_001'/*Buy close today execution*/,'205_002_002'/*Sell close today execution*/)
                                                       THEN REPLACE(V_DATE,'-')
                                                  ELSE SUBSTR(T.KCTIME,1,8) END
                         AND T.KCBSFLAG   = DECODE(V_BSFLAG, 'B', 'S', 'S', 'B')                                                
                         AND T.OFFERREGID = V_OFFERREGID--Speculation/hedging account
                         AND T.STKID      = V_STKID--Contract code
                         AND T.BRANCHID   = V_BRANCHID --Difference: Settlement statement close doesn't match open position account                        
                         AND T.ACCTID     = V_ACCTID   --Difference: Settlement statement close doesn't match open position account
                         AND T.UNCLOSETYPE = 1 --Close position flag: Closed=0, Unclosed=1
                         AND T.BASE_DATE = V_DATE
                       ORDER BY --SERIALNUM,
                                T.KCTIME,
                                T.KCCODE))
       WHERE RNK = 1;
    S_MODULE_NAME VARCHAR2(500); --Variable 5: Module name
    V_CNT  NUMBER(8);--Variable 6: Internal counter
    ------------------------------------Variable declaration END
  BEGIN
    --Write today's trading logs to temp table: TTRD_ACC_FUT_TRADINLOG_DAY
    S_MODULE_NAME := 'Temp table processing';
    --Delete temp table data
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE XIR_TRD.TTRD_ACC_FUT_TRADINLOG_DAY' ;--Faster, but will delete other dates' data when running simultaneously
    delete from XIR_TRD.TTRD_ACC_FUT_TRADINLOG_DAY 
    where SUBSTR(KNOCKTIME, 1, 8)>= REPLACE(P_BEG_DATE, '-', '')
    and SUBSTR(KNOCKTIME, 1, 8) <= REPLACE(P_END_DATE, '-', '');
    commit;
    --Write data to temp table
    INSERT INTO XIR_TRD.TTRD_ACC_FUT_TRADINLOG_DAY
      (KNOCKTIME,
       STKID,
       KNOCKQTY,
       KNOCKPRICE,
       KNOCKAMT,
       BSFLAG,
       F_OFFSETFLAG,
       RECKONINGFEE,
       ACCTID,
       KNOCKCODE,
       EXCHID,
       CUSTCOMMISION,
       EXCHCOMMISION,
       BRANCHID,
       OFFERREGID,
       SERIALNUM,
       BRIEFID,
       F_PRODUCTID)
      SELECT T.RECKONINGTIME, --Clearing time
             T.STKID, --Contract code
             T.KNOCKQTY, --Execution quantity
             T.KNOCKPRICE, --Execution price
             T.KNOCKAMT,--Execution amount
             T.BSFLAG,--Long/short flag
             T.F_OFFSETFLAG, --Open/close flag
             T.RECKONINGFEE, --Clearing fee
             T.ACCTID, --Fund account
             T.KNOCKCODE,--Execution number
             T.EXCHID, --Trading market
             T.CUSTCOMMISION, --Commission
             T.EXCHCOMMISION, --Exchange commission
             T.BRANCHID, --Department number
             T.OFFERREGID, --Speculation code
             SERIALNUM,--Auto-increment column
             BRIEFID, --Operation instruction
             F_PRODUCTID--Product code
        FROM XIR_TRD.VG_FUTURETRADINGLOG T--Genius futures trading log view in Hengtai CTSDB.FUTURETRADINGLOG2020@CTSSP->XIR_TRD.VG_FUTURETRADINGLOG
       WHERE 1=1
         --AND STKID = P_I_CODE--Test
         AND T.BRIEFID IN ('205_001_001', --Buy open execution
                           '205_001_002', --Sell open execution
                           '205_002_001', --Buy close today execution
                           '205_002_002', --Sell close today execution
                           '205_002_003', --Buy close yesterday execution
                           '205_002_004', --Sell close yesterday execution
                           '205_002_005', --Buy delivery execution
                           '205_002_006' --Sell delivery execution
                           )
        AND (T.EXCHID IN ('N', 'S', 'Z', 'D') --Commodity futures, determined by market
              OR T.F_PRODUCTID IN ('IC', 'IF', 'IM','IH', 'T', 'TS', 'TF')) --Stock index futures, treasury futures, determined by product ID
         AND SUBSTR(RECKONINGTIME, 1, 8) >= REPLACE(P_BEG_DATE, '-', '')
         AND SUBSTR(RECKONINGTIME, 1, 8) <= REPLACE(P_END_DATE, '-', '');
    COMMIT;

    --Only calculate trading day data
    FOR CR IN (SELECT CAL_DAY AS BASE_DATE
               FROM XIR_MD.TCALENDAR_DATES T--Calendar table
               WHERE T.CAL_CODE = 'CHINA_EX' --Exchange
               AND T.CAL_FLAG = 1 --Trading day
               AND T.CAL_DAY BETWEEN P_BEG_DATE AND P_END_DATE)
    LOOP
      --Get previous trading day: V_LAST_TRDDATE
      SELECT MAX(CAL_DAY) INTO  V_LAST_TRDDATE
             FROM  XIR_MD.TCALENDAR_DATES T
                 WHERE T.CAL_CODE = 'CHINA_EX' --Exchange
                    AND T.CAL_FLAG = 1 --Trading day
                    AND CAL_DAY<CR.BASE_DATE;

      V_DATE := CR.BASE_DATE;

      --Delete target table data by day
      DELETE FROM XIR_TRD.TTRD_ACC_FUT_TRDDETAIL WHERE BASE_DATE = V_DATE;
      COMMIT;

      --Write yesterday's unclosed positions to today's positions
      --DBMS_OUTPUT.PUT_LINE(V_DATE||'Write yesterday positions');--Print
      S_MODULE_NAME := 'Smooth previous day unclosed positions';
      INSERT INTO XIR_TRD.TTRD_ACC_FUT_TRDDETAIL
        (BASE_DATE,
         BRANCHID,
         ACCTID,
         OFFERREGID,
         STKID,
         EXCHID,
         CONTRACTTIMES,
         SETTLEMENTPRICE,
         PRESETTLEMENTPRICE,
         KCTIME,
         KCCODE,
         KCSL,
         KCJG,
         KCAMT, /*KCAVGPRICE,*/
         KCBSFLAG,
         KCF_OFFSETFLAG,
         COMMISION,
         UNCLOSEQTY,
         UNCLOSETYPE,
         F_PRODUCTID,
         COST_D,
         FDSY_D,
         AVGPRICE,
         KCCODE_RNK)
        SELECT T.BASE_DATE,
               T.BRANCHID,
               T.ACCTID,
               T.OFFERREGID,
               T.STKID,
               T.EXCHID,
               F.CONTRACTTIMES,
               F.SETTLEMENTPRICE,
               F.PRESETTLEMENTPRICE,
               T.KCTIME,
               T.KCCODE,
               T.KCSL,
               T.KCJG,
               T.KCAMT, /*T.KCAVGPRICE,*/
               T.KCBSFLAG,
               T.KCF_OFFSETFLAG,
               T.COMMISION,
               T.UNCLOSEQTY,
               T.UNCLOSETYPE,
               T.F_PRODUCTID,
               --Non-today open position daily cost: (previous settlement price) * quantity * multiplier
               (F.PRESETTLEMENTPRICE) * T.UNCLOSEQTY * F.CONTRACTTIMES AS COST_D,--Recalculate daily cost
               --Non-today open position daily floating P/L: (today's settlement price - previous settlement price) * quantity * multiplier
               DECODE(T.KCBSFLAG, 'B', 1, -1) * (F.SETTLEMENTPRICE - F.PRESETTLEMENTPRICE) * T.UNCLOSEQTY * F.CONTRACTTIMES AS FDSY_D,--Recalculate daily floating P/L
               T.AVGPRICE,
               T.KCCODE_RNK
          FROM (SELECT V_DATE AS BASE_DATE,--Today
                       T.BRANCHID,
                       T.ACCTID,
                       T.OFFERREGID,
                       T.STKID,
                       T.EXCHID,
                       T.KCTIME,
                       T.KCCODE,
                       T.KCSL,
                       T.KCJG,
                       T.KCAMT, /*KCAVGPRICE,*/
                       T.KCBSFLAG,
                       T.KCF_OFFSETFLAG,
                       0 AS COMMISION,--Commission only for today, not needed when smoothing to next day
                       T.UNCLOSEQTY,
                       T.UNCLOSETYPE,
                       T.F_PRODUCTID,
                       T.AVGPRICE,
                       T.KCCODE_RNK
                  FROM XIR_TRD.TTRD_ACC_FUT_TRDDETAIL T
                 WHERE T.UNCLOSETYPE = 1--Close position flag: Closed=0, Unclosed=1
                 AND BASE_DATE = V_LAST_TRDDATE--Get previous day
                   ) T
          LEFT JOIN (SELECT V_DATE AS OCCURTIME,
                            F.STKID,
                            F.EXCHID,
                            F.CONTRACTTIMES,
                            F.PRESETTLEMENTPRICE,
                            F.SETTLEMENTPRICE,--Rewrite latest settlement price
                            F.F_PRODUCTID
                       FROM XIR_TRD.VG_FUTUREINFO F--Futures market data table
                      WHERE  (F.EXCHID IN ('N', 'S', 'Z', 'D')--Commodity futures, determined by market
                             OR F.F_PRODUCTID IN ('IC', 'IF', 'IM','IH', 'T', 'TS', 'TF')) --Stock index futures, treasury futures, determined by product ID
                      AND SUBSTR(F.OCCURTIME, 1, 8) = REPLACE(V_DATE, '-','')--Get today
                      ) F
           ON  F.F_PRODUCTID = T.F_PRODUCTID
           AND F.EXCHID = T.EXCHID
           AND F.STKID = T.STKID
           AND F.OCCURTIME = T.BASE_DATE;
      COMMIT;

   FOR TRD_ROW IN TRD_CURSOR LOOP
   --DBMS_OUTPUT.PUT_LINE(V_DATE || 'Traverse trades'); --Print
   V_CNT:=TRD_CURSOR%ROWCOUNT;--Count trading log cursor records
        --Check open position: If open, write directly--
        IF TRD_ROW.F_OFFSETFLAG = 'OPEN' THEN
        S_MODULE_NAME := 'Open position processing';
          INSERT INTO XIR_TRD.TTRD_ACC_FUT_TRDDETAIL
            (BASE_DATE,
             BRANCHID,
             ACCTID,
             OFFERREGID,
             STKID,
             EXCHID,
             CONTRACTTIMES,
             SETTLEMENTPRICE,
             PRESETTLEMENTPRICE,
             KCTIME,
             KCCODE,
             KCSL,
             KCJG,
             KCAMT,
             KCBSFLAG,
             KCF_OFFSETFLAG,
             COMMISION,
             UNCLOSEQTY,
             UNCLOSETYPE,
             F_PRODUCTID,
             COST_D,
             FDSY_D,
             KCCODE_RNK
             )
          VALUES
            (TRD_ROW.BASE_DATE,
             TRD_ROW.BRANCHID,
             TRD_ROW.ACCTID,
             TRD_ROW.OFFERREGID,
             TRD_ROW.STKID,
             TRD_ROW.EXCHID,
             TRD_ROW.CONTRACTTIMES,
             TRD_ROW.SETTLEMENTPRICE,
             TRD_ROW.PRESETTLEMENTPRICE,
             TRD_ROW.KNOCKTIME,
             TRD_ROW.KNOCKCODE,
             TRD_ROW.KNOCKQTY,
             TRD_ROW.KNOCKPRICE,
             TRD_ROW.KNOCKAMT,
             TRD_ROW.BSFLAG,
             TRD_ROW.F_OFFSETFLAG,
             TRD_ROW.CUSTCOMMISION,
             TRD_ROW.KNOCKQTY,
             1,--Close position flag: Closed=0, Unclosed=1
             TRD_ROW.F_PRODUCTID,
              --Today's open position daily cost=open price * quantity * multiplier
             TRD_ROW.KNOCKPRICE * TRD_ROW.KNOCKQTY * TRD_ROW.CONTRACTTIMES,--Daily cost
             --Today's open position daily floating P/L=(settlement price - open price) * quantity * multiplier
             DECODE(TRD_ROW.BSFLAG, 'B', 1, -1) * (TRD_ROW.SETTLEMENTPRICE - TRD_ROW.KNOCKPRICE) * TRD_ROW.KNOCKQTY * TRD_ROW.CONTRACTTIMES,  --Daily floating P/L
             TRD_ROW.KCCODE_RNK
             );
          COMMIT;

      --Update moving average unit cost: cumulative open position cost/position quantity/contract multiplier
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_TRDDETAIL F
      USING (SELECT BASE_DATE,
                    T.STKID,
                   (SUM(NVL(AVGPRICE,0)*T.CONTRACTTIMES*T.UNCLOSEQTY)OVER(PARTITION BY STKID, KCBSFLAG, KCF_OFFSETFLAG, ACCTID, BRANCHID, OFFERREGID)--Historical cost
                    +TRD_ROW.KNOCKAMT)--This open position cost
                    / TRD_ROW.CONTRACTTIMES--Contract multiplier
                    / NULLIF(SUM(T.UNCLOSEQTY) OVER(PARTITION BY STKID, KCBSFLAG, KCF_OFFSETFLAG, ACCTID, BRANCHID, OFFERREGID),0) --Position quantity
                    AS AVGPRICE,  --Moving average unit cost
                    T.KCBSFLAG,
                    T.KCF_OFFSETFLAG,
                    T.ACCTID,
                    T.KCTIME,
                    T.KCCODE,
                    T.EXCHID,
                    T.F_PRODUCTID,
                    T.BRANCHID,
                    T.OFFERREGID
               FROM XIR_TRD.TTRD_ACC_FUT_TRDDETAIL T
               WHERE T.COMMISION IS NOT NULL--NULL means not today's open position
               AND T.ACCTID=TRD_ROW.ACCTID
               AND T.OFFERREGID=TRD_ROW.OFFERREGID
               AND T.KCBSFLAG=TRD_ROW.BSFLAG
               AND T.STKID=TRD_ROW.STKID
               AND T.BASE_DATE = V_DATE
               --AND UNCLOSETYPE = 1--Close position flag: Closed=0, Unclosed=1
               ) T
      ON (T.BASE_DATE = F.BASE_DATE
      AND T.KCTIME = F.KCTIME
      AND T.STKID = F.STKID
      AND T.ACCTID = F.ACCTID
      AND T.EXCHID = F.EXCHID
      AND T.F_PRODUCTID = F.F_PRODUCTID
      AND T.BRANCHID = F.BRANCHID
      AND T.OFFERREGID = F.OFFERREGID
      AND T.KCF_OFFSETFLAG = F.KCF_OFFSETFLAG
      AND T.KCBSFLAG = F.KCBSFLAG
      AND T.KCCODE = F.KCCODE
      AND F.COMMISION IS NOT NULL
      )
      WHEN MATCHED THEN
        UPDATE SET F.AVGPRICE = T.AVGPRICE;
      COMMIT;

          --Check close position: If close, traverse current position details using FIFO,
        ELSE
           S_MODULE_NAME := 'Close position processing';
          FOR POS_ROW IN POS_CURSOR(TRD_ROW.STKID,
                                    TRD_ROW.BSFLAG,
                                    TRD_ROW.BRIEFID,
                                    TRD_ROW.ACCTID,
                                    TRD_ROW.BRANCHID,
                                    TRD_ROW.OFFERREGID) LOOP
            --DBMS_OUTPUT.PUT_LINE(V_DATE || 'Process close position: P-K' || TRD_ROW.KNOCKCODE||'Close quantity:'||TRD_ROW.KNOCKQTY||'-----'||POS_ROW.KCCODE||'Open quantity:'||POS_ROW.KCSL); --Print
            UPDATE XIR_TRD.TTRD_ACC_FUT_TRDDETAIL FUT
               SET COMMISION      = NVL(FUT.COMMISION, 0) + TRD_ROW.CUSTCOMMISION, --Commission: close+open
                   PCTIME         = TRD_ROW.KNOCKTIME, --Close time
                   PCCODE         = TRD_ROW.KNOCKCODE, --Close number
                   PCSL           = TRD_ROW.KNOCKQTY, --Close data
                   PCJG           = TRD_ROW.KNOCKPRICE, --Close price
                   PCAMT          = TRD_ROW.KNOCKAMT, --Close amount
                   PCBSFLAG       = TRD_ROW.BSFLAG, --Close long/short flag
                   PCF_OFFSETFLAG = TRD_ROW.F_OFFSETFLAG, --Close flag
                   PCAVGPRICE     = POS_ROW.KCAVGPRICE, --Position average price, changes each time a position is closed
                   AVGPRICE       = POS_ROW.AVGPRICE,--Buy price moving average
                   FDSY_D = (POS_ROW.UNCLOSEQTY - TRD_ROW.KNOCKQTY)/POS_ROW.UNCLOSEQTY*POS_ROW.FDSY_D, --Daily floating P/L
                   COST_D = (POS_ROW.UNCLOSEQTY - TRD_ROW.KNOCKQTY)/POS_ROW.UNCLOSEQTY*POS_ROW.COST_D, --Daily cost
                   --Close position P/L: Per trade: close price - open price
                   PCSY_C = CASE WHEN TRD_ROW.BRIEFID IN ('205_002_005', '205_002_006') --Delivery check: Delivery: use today's settlement price as close price
                                 THEN NVL(PCSY_C, 0) + DECODE(FUT.KCBSFLAG, 'S', 1, -1) * (POS_ROW.KCJG - TRD_ROW.SETTLEMENTPRICE) * TRD_ROW.KNOCKQTY * TRD_ROW.CONTRACTTIMES
                            ELSE NVL(PCSY_C, 0) + DECODE(FUT.KCBSFLAG, 'S', 1, -1) * (POS_ROW.KCJG - TRD_ROW.KNOCKPRICE) * TRD_ROW.KNOCKQTY * CONTRACTTIMES
                            END,
                   --Close position P/L: Daily: close price - previous settlement price
                   PCSY_D = CASE WHEN TRD_ROW.BRIEFID IN ('205_002_005', '205_002_006') --Delivery check: Delivery: use today's settlement price as close price
                                 THEN NVL(PCSY_D, 0) + DECODE(FUT.KCBSFLAG, 'S', 1, -1) * (TRD_ROW.PRESETTLEMENTPRICE - TRD_ROW.SETTLEMENTPRICE) * TRD_ROW.KNOCKQTY * TRD_ROW.CONTRACTTIMES
                                 WHEN TRD_ROW.BRIEFID IN ('205_002_001', '205_002_002') --Today close position check: Genius daily today close same as per trade, using open price as cost, not previous settlement price---
                                 THEN  NVL(PCSY_D, 0) + DECODE(FUT.KCBSFLAG, 'S', 1, -1) * (POS_ROW.KCJG - TRD_ROW.KNOCKPRICE) * TRD_ROW.KNOCKQTY * CONTRACTTIMES
                            ELSE NVL(PCSY_D, 0) + DECODE(FUT.KCBSFLAG, 'S', 1, -1) * (TRD_ROW.PRESETTLEMENTPRICE - TRD_ROW.KNOCKPRICE) * TRD_ROW.KNOCKQTY * CONTRACTTIMES
                            END,
                   --Close position P/L: Average: close price - position average price
                   PCSY_A = CASE WHEN TRD_ROW.BRIEFID IN ('205_002_005', '205_002_006') --Delivery check: Delivery: use today's settlement price as close price
                                 THEN NVL(PCSY_D, 0) + DECODE(FUT.KCBSFLAG, 'S', 1, -1) * (POS_ROW.AVGPRICE - TRD_ROW.SETTLEMENTPRICE) * TRD_ROW.KNOCKQTY * TRD_ROW.CONTRACTTIMES
                            ELSE NVL(PCSY_D, 0) + DECODE(FUT.KCBSFLAG, 'S', 1, -1) * (POS_ROW.AVGPRICE - TRD_ROW.KNOCKPRICE) * TRD_ROW.KNOCKQTY * CONTRACTTIMES
                            END,
                   UNCLOSEQTY  = POS_ROW.UNCLOSEQTY - TRD_ROW.KNOCKQTY, --Unclosed quantity
                   UNCLOSETYPE = CASE WHEN /*UNCLOSEQTY-A_PC.PCSL=0*/ NVL(TRD_ROW.KNOCKQTY, 0) >= 1
                                      THEN 0
                                 ELSE 1
                                 END --Close position flag: Closed=0, Unclosed=1
             WHERE TRD_ROW.STKID = FUT.STKID
               AND TRD_ROW.ACCTID = FUT.ACCTID
               AND TRD_ROW.BRANCHID = FUT.BRANCHID
               AND TRD_ROW.OFFERREGID = FUT.OFFERREGID
               AND TRD_ROW.BSFLAG = DECODE(FUT.KCBSFLAG, 'B', 'S', 'S', 'B')
               AND POS_ROW.KCCODE = FUT.KCCODE
               AND POS_ROW.BASE_DATE = FUT.BASE_DATE
               AND POS_ROW.KCTIME = FUT.KCTIME
               AND FUT.UNCLOSETYPE = 1--Close position flag: Closed=0, Unclosed=1
               AND FUT.BASE_DATE = V_DATE;
            COMMIT;
          --For positions not fully closed, write again, update position floating P/L, cost, wait for next close
            IF POS_ROW.UNCLOSEQTY - TRD_ROW.KNOCKQTY <> 0 THEN
              INSERT INTO XIR_TRD.TTRD_ACC_FUT_TRDDETAIL
                (BASE_DATE,
                 STKID,
                 ACCTID,
                 BRANCHID,
                 OFFERREGID,
                 KCBSFLAG,
                 KCCODE,
                 KCTIME,
                 KCJG,
                 KCSL,
                 KCAMT,
                 KCF_OFFSETFLAG,
                 EXCHID,
                 CONTRACTTIMES,
                 PRESETTLEMENTPRICE,
                 SETTLEMENTPRICE,
                 --COMMISION,
                 UNCLOSEQTY,
                 UNCLOSETYPE,
                 F_PRODUCTID,
                 COST_D,
                 FDSY_D,
                 KCCODE_RNK,
                 AVGPRICE
                 )
              VALUES
                (V_DATE,
                 POS_ROW.STKID,
                 POS_ROW.ACCTID,
                 POS_ROW.BRANCHID,
                 POS_ROW.OFFERREGID,
                 POS_ROW.KCBSFLAG,
                 POS_ROW.KCCODE,
                 POS_ROW.KCTIME,
                 POS_ROW.KCJG,
                 POS_ROW.KCSL,
                 POS_ROW.KCAMT,
                 POS_ROW.KCF_OFFSETFLAG,
                 POS_ROW.EXCHID,
                 POS_ROW.CONTRACTTIMES,
                 POS_ROW.PRESETTLEMENTPRICE,
                 POS_ROW.SETTLEMENTPRICE,
                 POS_ROW.UNCLOSEQTY - TRD_ROW.KNOCKQTY,--Unclosed quantity
                 1,--Close position flag: Closed=0, Unclosed=1
                 POS_ROW.F_PRODUCTID,
                 --Today's unclosed positions: today's open positions, historical positions
                 (POS_ROW.UNCLOSEQTY - TRD_ROW.KNOCKQTY)/POS_ROW.UNCLOSEQTY*POS_ROW.COST_D,--Daily floating P/L
                 (POS_ROW.UNCLOSEQTY - TRD_ROW.KNOCKQTY)/POS_ROW.UNCLOSEQTY*POS_ROW.FDSY_D,--Daily floating P/L
                 POS_ROW.KCCODE_RNK+1,--Increment
                 POS_ROW.AVGPRICE);
              COMMIT;
            END IF;--End unclosed position writing

          END LOOP; --End POS_ROW loop, close position processing complete

        END IF;--End open/close position check
    END LOOP; --End position TRD_ROW loop, today's open/close processing complete

      --After in/out position processing, update current position average price
      S_MODULE_NAME := 'Update latest position average price';
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_TRDDETAIL F
      USING (SELECT BASE_DATE,
                    T.STKID,
                    CASE WHEN UNCLOSETYPE = 1--Unclosed
                    THEN ROUND(NVL(SUM(NVL(KCAVGPRICE, 0)) OVER(PARTITION BY  BASE_DATE, STKID, ACCTID, BRANCHID,OFFERREGID,KCBSFLAG), 0), 8)
                    ELSE NULL END AS KCAVGPRICE, --Position average price
                    T.KCBSFLAG,
                    T.KCF_OFFSETFLAG,
                    T.ACCTID,
                    T.KCTIME,
                    T.KCCODE,
                    T.PCCODE,
                    T.EXCHID,
                    T.F_PRODUCTID,
                    T.BRANCHID,
                    T.OFFERREGID,
                    T.UNCLOSEQTY,
                    PCCODE_RNK
               FROM (SELECT BASE_DATE,
                            T.STKID,
                            NVL(KCJG, 0) * RATIO_TO_REPORT(DECODE( UNCLOSETYPE,1 , NVL(UNCLOSEQTY, 0), 0)) OVER(PARTITION BY BASE_DATE, STKID, ACCTID, BRANCHID, OFFERREGID, KCBSFLAG) AS KCAVGPRICE, --Calculate position average price
                            T.KCBSFLAG,
                            T.KCF_OFFSETFLAG,
                            T.ACCTID,
                            T.KCTIME,
                            T.KCCODE,
                            T.PCCODE,
                            T.EXCHID,
                            T.F_PRODUCTID,
                            T.BRANCHID,
                            T.OFFERREGID,
                            T.UNCLOSEQTY,
                            T.UNCLOSETYPE,
                            --Add PCCODE sequence number, when multiple close codes exist, sort internally
                            CASE WHEN UNCLOSETYPE = 0 --Closed
                            THEN ROW_NUMBER() OVER(PARTITION BY T.BASE_DATE,PCCODE ORDER BY KCCODE)
                            ELSE NULL END AS PCCODE_RNK
                       FROM XIR_TRD.TTRD_ACC_FUT_TRDDETAIL T
                      WHERE T.BASE_DATE = V_DATE
                        --AND UNCLOSETYPE = 1--Close position flag: Closed=0, Unclosed=1
                        ) T
               ) T
      ON (T.BASE_DATE = F.BASE_DATE AND
      T.KCTIME = F.KCTIME AND
      T.STKID = F.STKID AND
      T.ACCTID = F.ACCTID AND
      T.EXCHID = F.EXCHID AND
      T.F_PRODUCTID = F.F_PRODUCTID AND
      T.BRANCHID = F.BRANCHID AND
      T.OFFERREGID = F.OFFERREGID AND
      T.KCF_OFFSETFLAG = F.KCF_OFFSETFLAG AND
      T.KCBSFLAG = F.KCBSFLAG AND
      T.KCCODE = F.KCCODE AND
      NVL(T.PCCODE,1) = NVL(F.PCCODE,1) AND--Unclosed has no KCCODE
      T.UNCLOSEQTY=F.UNCLOSEQTY
      )
      WHEN MATCHED THEN
        UPDATE SET F.KCAVGPRICE = T.KCAVGPRICE,
                   F.PCCODE_RNK = T.PCCODE_RNK;
      COMMIT;
 -------
--V2 Write settlement statement, summarized P/L by PCCODE
--Delivery TTRD_SRC_DELIVERY_RECORD_ZT has no number so cannot match
/*S_MODULE_NAME := 'Write settlement statement close position info';
   MERGE INTO TTRD_ACC_FUT_TRDDETAIL F--Detail table
      USING ( SELECT F.BASE_DATE,
                     F.PCCODE,
                     T.PCBH,
                     T.KCBH AS JSD_KCBH, --Settlement statement_Open position number
                     JSD_PCSY* F.CONTRACTTIMES AS JSD_PCSY, --Settlement statement-Per trade P/L
                     --Difference with settlement statement
                     JSD_PCSY* F.CONTRACTTIMES - F.PCSY_C AS DIFF_SY, --P/L difference: settlement statement - actual
                     T.KCJG - F.KCJG AS DIFF_PRI --Price difference: settlement statement - actual
               FROM (SELECT F.BASE_DATE,
                           F.STKID,
                           F.PCCODE,
                           F.CONTRACTTIMES,
                           F.PCBSFLAG,
                           AVG(F.KCJG) AS KCJG,
                           SUM(F.PCSY_C) AS PCSY_C
                      FROM TTRD_ACC_FUT_TRDDETAIL F
                     WHERE UNCLOSETYPE = 0 --Today's closed positions
                       AND BASE_DATE = V_DATE
                     GROUP BY F.BASE_DATE, F.STKID, F.PCCODE, F.CONTRACTTIMES, F.PCBSFLAG--,KCCODE
                     )F
                 LEFT JOIN (
                      SELECT T.BEG_DATE,
                            T.HYDM,
                            T.PCBH,
                            --T.KCBH,
                            (SELECT KCBH FROM TTRD_SRC_OFFSET_GAIN_LOSS_ZT ZT WHERE ZT.BEG_DATE=T.BEG_DATE AND ZT.HYDM=T.HYDM AND ZT.PCBH=T.PCBH AND ROWNUM<=1) AS KCBH,--Take one KCBH
                            AVG(KCJG)AS KCJG,
                            SUM(DECODE(TRANSLATE(MM, '买卖', 'BS'), 'S', 1, -1) * (T.PCJG - KCJG) * T.SHOUSHU)  AS JSD_PCSY,
                            TRANSLATE(MM, '买卖', 'BS') AS BS
                       FROM XIR_TRD.TTRD_SRC_OFFSET_GAIN_LOSS_ZT T--Settlement statement import has duplicate data
                      WHERE FILENAME LIKE '%Mark-to-market%'
                        AND BEG_DATE =V_DATE
                      GROUP BY BEG_DATE,HYDM,PCBH,TRANSLATE(MM, '买卖', 'BS'))T
             ON F.STKID = T.HYDM
            AND F.PCCODE = T.PCBH
            AND F.BASE_DATE = T.BEG_DATE
            AND F.PCBSFLAG = T.BS
) T
      ON (F.BASE_DATE = T.BASE_DATE AND F.PCCODE = T.PCCODE AND F.PCCODE_RNK=1 AND UNCLOSETYPE = 0)
      WHEN MATCHED THEN
        UPDATE
           SET F.DIFF_PRI = T.DIFF_PRI,
               F.JSD_KCBH = T.JSD_KCBH,
               F.JSD_PCSY = T.JSD_PCSY,--Cannot include delivery, temporarily adjusted
               F.DIFF_SY  = T.DIFF_SY;
      COMMIT;*/
      
      --After daily close, write results to summary table: TTRD_ACC_FUT_BALANCE
      DELETE FROM XIR_TRD.TTRD_ACC_FUT_BALANCE WHERE BASE_DATE = V_DATE;
      COMMIT;     
      S_MODULE_NAME := 'Summarize detail data';

      INSERT INTO XIR_TRD.TTRD_ACC_FUT_BALANCE --Summary table
        (BASE_DATE,
         BRANCHID,
         EXTMAGCARDID,
         ACCTID,
         OFFERREGID,
         STKID,
         F_PRODUCTID,
         F_PRODUCTTYPE,
         EXCHID,
         BSFLAG,
         QTY,
         CONTRACTTIMES,
         SETTLEMENTPRICE,
         PRESETTLEMENTPRICE,
         KCAVGPRICE,
         AVGPRICE,
         COST_A,
         COST_C,
         COST_D,
         MK,
         FDSY_A,
         FDSY_C,
         FDSY_D,
         DAY_SXF,
         DAY_PCSY_C,
         --DAY_JSD_PCSY,
         --DIFF_PCSY,
         DAY_PCSY_D,
         DAY_PCSY_A)
        SELECT T.BASE_DATE, --Date
               T.BRANCHID, --Department number
               ACC.EXTMAGCARDID,--Futures company account
               T.ACCTID, --Fund account
               T.OFFERREGID, --Speculation/hedging code
               T.STKID, --Contract code
               T.F_PRODUCTID, --Product code
               CASE WHEN F_PRODUCTID IN ('IC', 'IF', 'IH','IM')
                    THEN 'Stock Index Futures'
                    WHEN F_PRODUCTID IN ('T', 'TS', 'TF')
                    THEN 'Treasury Futures'
               ELSE 'Commodity Futures'
               END AS F_PRODUCTTYPE, --Product type
               T.EXCHID, --Market
               T.KCBSFLAG AS BSFLAG, --Long/Short
               SUM(DECODE(UNCLOSETYPE,0,0,NVL(UNCLOSEQTY, 0))) AS QTY, --Position quantity
               AVG(CONTRACTTIMES)  AS CONTRACTTIMES, --Contract multiplier
               AVG(SETTLEMENTPRICE) AS SETTLEMENTPRICE, --Today's settlement price
               AVG(PRESETTLEMENTPRICE) AS PRESETTLEMENTPRICE, --Previous settlement price
               NVL(ROUND(AVG(KCAVGPRICE), 8),0) AS KCAVGPRICE, --Per trade moving average price  --Verify with settlement statement, Genius
               NVL(ROUND(AVG(DECODE(UNCLOSETYPE,0,NULL,AVGPRICE)), 8),0) AS AVGPRICE,  --Per trade position average price  --Verify with Hengtai
               --Open position average price keeps 8 decimal places, average price calculations need ROUND
               --Current
               ROUND(SUM(DECODE(UNCLOSETYPE,0,0,AVGPRICE * UNCLOSEQTY * CONTRACTTIMES)),0) AS COST_A,   --Per trade moving average cost   --Verify with Hengtai
               ROUND(SUM(DECODE(UNCLOSETYPE,0,0,KCAVGPRICE * UNCLOSEQTY * CONTRACTTIMES)),0) AS COST_C, --Per trade position cost
               SUM(DECODE(UNCLOSETYPE,0,0,NVL(COST_D, 0))) AS COST_D, --Daily cost
               --SUM(DECODE(UNCLOSETYPE,0,0,PRESETTLEMENTPRICE * UNCLOSEQTY * CONTRACTTIMES)) AS COST_D, --Daily cost, today's open needs separate check, all calculated in detail table
               SUM(DECODE(UNCLOSETYPE,0,0,SETTLEMENTPRICE * UNCLOSEQTY * CONTRACTTIMES)) AS MK, --Market value
               --Position floating P/L
               ROUND(SUM(DECODE(UNCLOSETYPE,0,0,DECODE(KCBSFLAG, 'B', 1, -1) * (SETTLEMENTPRICE - AVGPRICE) * UNCLOSEQTY * CONTRACTTIMES)),2) AS FDSY_A, --Per trade floating P/L-Position average price as cost     --Verify with Hengtai
               ROUND(SUM(DECODE(UNCLOSETYPE,0,0,DECODE(KCBSFLAG, 'B', 1, -1) * (SETTLEMENTPRICE - KCAVGPRICE) * UNCLOSEQTY * CONTRACTTIMES)),0) AS FDSY_C, --Per trade floating P/L-Position average price as cost --Verify with settlement statement, Genius
               SUM(NVL(DECODE(UNCLOSETYPE,0,0,FDSY_D),0)) AS FDSY_D,--Daily floating P/L-Previous settlement price as cost   Today's open needs separate check, all calculated in detail table
               --Today
               SUM(NVL(COMMISION, 0)) AS DAY_SXF, --Commission
               SUM(NVL(PCSY_C, 0)) AS DAY_PCSY_C, --Per trade_Close position P/L-Open price as cost  --Verify with settlement statement, Genius
               --SUM(NVL(JSD_PCSY,0))AS DAY_JSD_PCSY,--Settlement statement per trade close P/L
               --SUM(NVL(DIFF_SY,0)) AS DIFF_PCSY,  --Difference with settlement statement P/L
               SUM(NVL(PCSY_D, 0)) AS DAY_PCSY_D, --Daily_Close position P/L-Previous settlement price as cost
               ROUND(SUM(NVL(PCSY_A, 0)),2) AS DAY_PCSY_A --Close position P/L-Position average price as cost    --Verify with Hengtai
          FROM XIR_TRD.TTRD_ACC_FUT_TRDDETAIL T
          LEFT JOIN (--Get futures company account
                     SELECT DISTINCT --V_DATE AS BASE_DATE,
                            EXTMAGCARDID,--Futures company account
                            ACCTID--Genius account
                     FROM XIR_TRD.VG_ACCOUNT F
                     WHERE SUBSTR(OCCURTIME,1,4)=SUBSTR(V_DATE,1,4)
                     AND EXTMAGCARDID IS NOT NULL) ACC
          ON /*ACC.BASE_DATE=T.BASE_DATE
          AND*/ ACC.ACCTID=T.ACCTID
         WHERE T.BASE_DATE = V_DATE
        --AND UNCLOSETYPE=0--已平仓的
         GROUP BY T.BASE_DATE,
                  T.STKID,
                  T.EXCHID,
                  T.F_PRODUCTID,
                  T.KCBSFLAG,
                  T.BRANCHID,
                  ACC.EXTMAGCARDID,
                  T.ACCTID,
                  T.OFFERREGID;
      COMMIT;

  S_MODULE_NAME := '汇总表记录结算单盈亏';
  --加了之后速度变慢了
MERGE INTO XIR_TRD.TTRD_ACC_FUT_BALANCE F
     USING(    
      SELECT F.BASE_DATE,--Date
             F.EXTMAGCARDID,--Futures company account
             F.ACCTID,--Fund account
             F.OFFERREGID,--Speculation/hedging code
             F.STKID,--Contract code
             F.BSFLAG,--Long/Short flag
             QTY,
             --Closed position P/L
             --CLOSE_PL_C, --Settlement statement closed position P/L
             --DAY_PCSY_C,--Landed floating P/L
             ROUND(NVL(B.CLOSE_PL_C, 0)*RATIO_TO_REPORT(NVL(NULLIF(QTY,0),1))OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID),0)  AS CLOSE_PL_C,
             ROUND((NVL(B.CLOSE_PL_C, 0) - NVL(F.DAY_PCSY_C, 0)) *RATIO_TO_REPORT(NVL(NULLIF(QTY,0),1))OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID),0) AS DIFF_PCSY,--Closed position P/L difference --Distribute total difference by position ratio  May have rounding errors                 
             --(NVL(B.CLOSE_PL_C, 0) - NVL(F.DAY_PCSY_C, 0)) / COUNT(1) OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID) AS DIFF_PCSY,--Closed position P/L difference  Evenly distribute  
             --Floating P/L
             --NVL(B.BOOK_PL_C, 0) AS BOOK_PL_C,--Settlement statement floating P/L
             --F.FDSY_C,--Landed per trade floating P/L                                
             ROUND(NVL(B.BOOK_PL_C, 0)*RATIO_TO_REPORT(NVL(NULLIF(QTY,0),1))OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID),0)  AS BOOK_PL_C,
             ROUND((NVL(B.BOOK_PL_C, 0) - NVL(F.FDSY_C, 0))*RATIO_TO_REPORT(NVL(NULLIF(QTY,0),1))OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID),0) AS DIFF_FDSY,--Floating P/L difference--Distribute total difference by position ratio  May have rounding errors 
             --(NVL(B.BOOK_PL_C, 0) - NVL(F.FDSY_C, 0)) / COUNT(1) OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID) AS DIFF_FDSY--Floating P/L difference  Evenly distribute
             --Cost
             ROUND(NVL(B.cost_c, 0)*RATIO_TO_REPORT(NVL(NULLIF(QTY,0),1))OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID),0)  AS cost_c,
             ROUND((NVL(B.cost_c, 0) - NVL(F.cost_c, 0))*RATIO_TO_REPORT(NVL(NULLIF(QTY,0),1))OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID),0) AS DIFF_cost--Floating P/L difference--Distribute total difference by position ratio  May have rounding errors 
        FROM (SELECT BEG_DATE,
                     ACCID,
                     I_CODE,
                     TRANSLATE(LS, 'LS', 'BS') AS LS,
                     sum(nvl(cost_c,0)) as cost_c,--Position cost
                     SUM(NVL(BOOK_PL_C,0)) AS BOOK_PL_C,--Per trade floating P/L
                     SUM(nvl(CLOSE_PL_C,0)) AS CLOSE_PL_C--Per trade position P/L
                FROM XIR_TRD.VTRD_FUT_JSD_BALANCE--Settlement statement P/L view
               WHERE BEG_DATE = V_DATE
               --PKG_VIEW_PARAM.SET_BEGDATE(V_DATE)=V_DATE--Changed to parameter passing didn't make it faster
               --and A_TYPE <>'OPT_F'
               GROUP BY BEG_DATE, ACCID, LS, I_CODE
               ) B
       INNER JOIN (SELECT BASE_DATE,
                          EXTMAGCARDID,
                          BSFLAG,
                          ACCTID,
                          STKID,
                          OFFERREGID,
                          QTY,
                          --DAY_PCSY_C,
                          --FDSY_C,
                          SUM(cost_c) OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID) AS cost_c,
                          SUM(DAY_PCSY_C) OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID) AS DAY_PCSY_C,                         
                          SUM(FDSY_C) OVER(PARTITION BY BASE_DATE, EXTMAGCARDID, BSFLAG, STKID) AS FDSY_C
                     FROM XIR_TRD.TTRD_ACC_FUT_BALANCE--Landed P/L summary table
                    WHERE BASE_DATE = V_DATE
                      ) F
          ON B.LS = F.BSFLAG
         AND B.ACCID = F.EXTMAGCARDID --Futures company account
         AND B.BEG_DATE = F.BASE_DATE
         AND UPPER(B.I_CODE) = UPPER(F.STKID)               
      )T
    ON( F.BASE_DATE=T.BASE_DATE
    AND F.EXTMAGCARDID=T.EXTMAGCARDID
    AND F.ACCTID=T.ACCTID
    AND F.OFFERREGID=T.OFFERREGID
    AND F.STKID=T.STKID
    AND F.BSFLAG=T.BSFLAG)
    WHEN MATCHED THEN UPDATE SET
        F.JSD_cost=T.cost_c,--Settlement statement cost
        F.DIFF_cost=T.DIFF_cost,--Cost difference
        F.DAY_JSD_PCSY=T.CLOSE_PL_C,--Settlement statement daily close P/L
        F.DIFF_PCSY=T.DIFF_PCSY,--Daily close P/L difference
        F.JSD_FDSY=T.BOOK_PL_C,--Settlement statement floating P/L
        F.DIFF_FDSY=T.DIFF_FDSY;--Floating P/L difference
    COMMIT;
    --Update differences to summary table
    UPDATE XIR_TRD.TTRD_ACC_FUT_BALANCE F SET
      F.cost_AD=NVL(cost_C,0)+NVL(F.DIFF_cost,0),
      F.PCSY_AD=NVL(DAY_PCSY_C,0)+NVL(F.DIFF_PCSY,0)/*CASE WHEN NVL(F.DIFF_PCSY,0)=0 THEN 0
                                  ELSE NVL(F.DIFF_FDSY,0) END*/,
      F.FDSY_AD=NVL(FDSY_C,0)+NVL(F.DIFF_FDSY,0)
    WHERE BASE_DATE = V_DATE;
    COMMIT;


      S_MODULE_NAME := 'Update declaration fee';
      --Update declaration fee
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_BALANCE B
      USING (SELECT V_DATE AS BASE_DATE,
                    BRANCHID,
                    ACCTID,
                    OFFERREGID,
                    STKID,
                    F_PRODUCTID,
                    EXCHID,
                    CASE F_OFFSETFLAG || BSFLAG
                    WHEN 'OPENB' THEN 'B'
                    WHEN 'CLOSES'THEN 'B'
                    WHEN 'OPENS' THEN 'S'
                    WHEN 'CLOSEB'THEN 'S'
                    END AS BSFLAG,--CFFEX F_OFFSETFLAG only has CLOSE, OPEN 2 types
                    NVL(COUNT(1), 0) AS DAY_SBF
               FROM XIR_TRD.VG_FUTUREOPENORDER F--Futures order execution table
              WHERE EXCHID = 'F' --CFFEX
                AND VALIDFLAG = 0
                AND F_PRODUCTID IN ('IC', 'IF', 'IH','IM')
                AND SUBSTR(F.OCCURTIME, 1, 8) = REPLACE(V_DATE, '-')
              GROUP BY BRANCHID,
                       ACCTID,
                       OFFERREGID,
                       STKID,
                       F_PRODUCTID,
                       EXCHID,
                       CASE F_OFFSETFLAG || BSFLAG
                       WHEN 'OPENB' THEN 'B'
                       WHEN 'CLOSES'THEN 'B'
                       WHEN 'OPENS' THEN 'S'
                       WHEN 'CLOSEB'THEN 'S'
                       END) T
      ON (T.BASE_DATE = B.BASE_DATE AND T.BRANCHID = B.BRANCHID AND T.ACCTID = B.ACCTID AND T.OFFERREGID = B.OFFERREGID AND T.STKID = B.STKID AND T.F_PRODUCTID = B.F_PRODUCTID AND T.EXCHID = B.EXCHID AND T.BSFLAG = B.BSFLAG)
      WHEN MATCHED THEN
        UPDATE SET B.DAY_SBF = T.DAY_SBF;
      COMMIT;
            
      S_MODULE_NAME := 'Update margin';
      
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_BALANCE B
      USING ( SELECT V_DATE AS BASE_DATE,
                      BRANCHID,
                      ACCTID,
                      OFFERREGID,
                      STKID,
                      F_PRODUCTID,
                      EXCHID,
                      BSFLAG,
                      sum(f.MARGINUSEDAMT)as MARGINUSEDAMT
              FROM XIR_TRD.VG_FUTUREPOSITION F
              WHERE ( EXCHID IN ('N', 'S', 'Z', 'D')--Commodity futures
                   OR F_PRODUCTID IN ('IC','IF','IH','IM','T','TS','TF')) --Stock index futures, treasury futures   
              and SUBSTR(OCCURTIME, 1, 8) = REPLACE(V_DATE, '-')          
              GROUP BY BRANCHID,
                     ACCTID,
                     OFFERREGID,
                     STKID,
                     F_PRODUCTID,
                     EXCHID,
                     BSFLAG) T
      ON (T.BASE_DATE = B.BASE_DATE AND T.BRANCHID = B.BRANCHID AND T.ACCTID = B.ACCTID AND T.OFFERREGID = B.OFFERREGID AND T.STKID = B.STKID AND T.F_PRODUCTID = B.F_PRODUCTID AND T.EXCHID = B.EXCHID AND T.BSFLAG = B.BSFLAG)
      WHEN MATCHED THEN
        UPDATE SET B.MARGINUSEDAMT = T.MARGINUSEDAMT;
      COMMIT; 

      S_MODULE_NAME := 'Calculate daily floating P/L';
      
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_BALANCE B
      USING (SELECT BASE_DATE,BRANCHID,ACCTID,OFFERREGID,STKID,F_PRODUCTID,EXCHID,BSFLAG,DAY_FDSY_A,DAY_FDSY_C,DAY_FDSY_D
             FROM (SELECT BASE_DATE, --Date
                          BRANCHID, --Department number
                          ACCTID, --Fund account
                          OFFERREGID, --Speculation/hedging code
                          STKID, --Contract code
                          F_PRODUCTID,
                          EXCHID, --Market
                          BSFLAG, --Long/Short
                          --Daily floating P/L: today's floating P/L - yesterday's floating P/L
                          FDSY_A - LAG(FDSY_A, 1, 0) OVER(PARTITION BY STKID, EXCHID, F_PRODUCTID, BSFLAG, BRANCHID, ACCTID, OFFERREGID ORDER BY BASE_DATE) AS DAY_FDSY_A,
                          FDSY_C - LAG(FDSY_C, 1, 0) OVER(PARTITION BY STKID, EXCHID, F_PRODUCTID, BSFLAG, BRANCHID, ACCTID, OFFERREGID ORDER BY BASE_DATE) AS DAY_FDSY_C,
                          --Daily floating P/L, today's calculation doesn't subtract previous day
                          FDSY_D  AS DAY_FDSY_D
                     FROM XIR_TRD.TTRD_ACC_FUT_BALANCE
                    WHERE BASE_DATE >= V_LAST_TRDDATE--Previous trading day
                      AND BASE_DATE <= V_DATE)
              WHERE BASE_DATE = V_DATE) T
      ON (T.BASE_DATE = B.BASE_DATE AND T.BRANCHID = B.BRANCHID AND T.ACCTID = B.ACCTID AND T.OFFERREGID = B.OFFERREGID AND T.STKID = B.STKID AND T.F_PRODUCTID = B.F_PRODUCTID AND T.EXCHID = B.EXCHID AND T.BSFLAG = B.BSFLAG)
      WHEN MATCHED THEN
        UPDATE
           SET B.DAY_FDSY_A   = T.DAY_FDSY_A,
               B.DAY_FDSY_D   = T.DAY_FDSY_D,
               B.DAY_FDSY_C   = T.DAY_FDSY_C;
      COMMIT;

     S_MODULE_NAME := 'Calculate daily total P/L';

      MERGE INTO XIR_TRD.TTRD_ACC_FUT_BALANCE B
      USING ( SELECT BASE_DATE, --Date
                    BRANCHID, --Department number
                    ACCTID, --Fund account
                    OFFERREGID, --Speculation/hedging code
                    STKID, --Contract code
                    F_PRODUCTID,
                    EXCHID, --Market
                    BSFLAG, --Long/Short
                    DAY_FDSY_A+DAY_PCSY_A  AS DAY_PNL_A,
                    DAY_FDSY_D+DAY_PCSY_D  AS DAY_PNL_D,
                    DAY_FDSY_C+DAY_PCSY_C  AS DAY_PNL_C
               FROM XIR_TRD.TTRD_ACC_FUT_BALANCE
              WHERE BASE_DATE = V_DATE ) T
      ON (T.BASE_DATE = B.BASE_DATE AND T.BRANCHID = B.BRANCHID AND T.ACCTID = B.ACCTID AND T.OFFERREGID = B.OFFERREGID AND T.STKID = B.STKID AND T.F_PRODUCTID = B.F_PRODUCTID AND T.EXCHID = B.EXCHID AND T.BSFLAG = B.BSFLAG)
      WHEN MATCHED THEN
        UPDATE
           SET B.DAY_PNL_A = T.DAY_PNL_A,
               B.DAY_PNL_D = T.DAY_PNL_D,
               B.DAY_PNL_C = T.DAY_PNL_C;
      COMMIT;

      S_MODULE_NAME := 'Calculate monthly cumulative numbers';

      MERGE INTO XIR_TRD.TTRD_ACC_FUT_BALANCE B
      USING (SELECT V_DATE AS BASE_DATE, --Date
                    BRANCHID, --Department number
                    EXTMAGCARDID,--Futures company account
                    ACCTID, --Fund account
                    OFFERREGID, --Speculation/hedging code
                    STKID, --Contract code
                    F_PRODUCTID,
                    F_PRODUCTTYPE,
                    EXCHID, --Market
                    BSFLAG, --Long/Short
                    --Current month
                    SUM(NVL(DAY_SBF, 0)) AS MONTH_SBF, --Declaration fee
                    SUM(NVL(DAY_SXF, 0)) AS MONTH_SXF, --Commission
                    SUM(NVL(DAY_FDSY_C, 0)) AS MONTH_FDSY_C,
                    SUM(NVL(DAY_FDSY_D, 0)) AS MONTH_FDSY_D,
                    SUM(NVL(DAY_FDSY_A, 0)) AS MONTH_FDSY_A,
                    SUM(NVL(DAY_PCSY_C, 0)) AS MONTH_PCSY_C, --Close position P/L-Open price as cost
                    SUM(NVL(DAY_PCSY_D, 0)) AS MONTH_PCSY_D, --Close position P/L-Previous settlement price as cost
                    SUM(NVL(DAY_PCSY_A, 0)) AS MONTH_PCSY_A --Close position P/L-Position average price as cost
               FROM XIR_TRD.TTRD_ACC_FUT_BALANCE
              WHERE BASE_DATE <= V_DATE
                AND SUBSTR(BASE_DATE, 1, 7) = SUBSTR(V_DATE, 1, 7) --Current month
              GROUP BY SUBSTR(BASE_DATE, 1, 7),
                       STKID,
                       EXCHID,
                       F_PRODUCTID,
                       F_PRODUCTTYPE,
                       BSFLAG,
                       BRANCHID,
                       EXTMAGCARDID,
                       ACCTID,
                       OFFERREGID) T
      ON (T.BASE_DATE = B.BASE_DATE AND T.BRANCHID = B.BRANCHID AND T.ACCTID = B.ACCTID AND T.OFFERREGID = B.OFFERREGID AND T.STKID = B.STKID AND T.F_PRODUCTID = B.F_PRODUCTID AND T.F_PRODUCTTYPE = B.F_PRODUCTTYPE AND T.EXCHID = B.EXCHID AND T.BSFLAG = B.BSFLAG)
      WHEN MATCHED THEN
        UPDATE
           SET B.MONTH_SXF    = T.MONTH_SXF,
               B.MONTH_SBF    = T.MONTH_SBF,
               B.MONTH_FDSY_C = T.MONTH_FDSY_C,
               B.MONTH_FDSY_D = T.MONTH_FDSY_D,
               B.MONTH_FDSY_A = T.MONTH_FDSY_A,
               B.MONTH_PCSY_C = T.MONTH_PCSY_C,
               B.MONTH_PCSY_D = T.MONTH_PCSY_D,
               B.MONTH_PCSY_A = T.MONTH_PCSY_A
      WHEN NOT MATCHED THEN
        INSERT
          (BASE_DATE,
           BRANCHID,
           EXTMAGCARDID,
           ACCTID,
           OFFERREGID,
           STKID,
           F_PRODUCTID,
           F_PRODUCTTYPE,
           EXCHID,
           BSFLAG,
           MONTH_SBF,
           MONTH_SXF,
           MONTH_FDSY_C,
           MONTH_FDSY_D,
           MONTH_FDSY_A,
           MONTH_PCSY_C,
           MONTH_PCSY_D,
           MONTH_PCSY_A)
        VALUES
          (T.BASE_DATE,
           T.BRANCHID,
           T.EXTMAGCARDID,
           T.ACCTID,
           T.OFFERREGID,
           T.STKID,
           T.F_PRODUCTID,
           T.F_PRODUCTTYPE,
           T.EXCHID,
           T.BSFLAG,
           T.MONTH_SBF,
           T.MONTH_SXF,
           T.MONTH_FDSY_C,
           T.MONTH_FDSY_D,
           T.MONTH_FDSY_A,
           T.MONTH_PCSY_C,
           T.MONTH_PCSY_D,
           T.MONTH_PCSY_A);
      COMMIT;

      S_MODULE_NAME := 'Calculate yearly cumulative numbers';
      --Calculate yearly cumulative numbers
      MERGE INTO XIR_TRD.TTRD_ACC_FUT_BALANCE B
      USING (SELECT V_DATE AS BASE_DATE, --Date
                    BRANCHID, --Department number
                    EXTMAGCARDID,--Futures company account
                    ACCTID, --Fund account
                    OFFERREGID, --Speculation/hedging code
                    STKID, --Contract code
                    F_PRODUCTID,
                    F_PRODUCTTYPE,
                    EXCHID, --Market
                    BSFLAG, --Long/Short
                    --Current month
                    SUM(NVL(DAY_SBF, 0)) AS YEAR_SBF, --Declaration fee
                    SUM(NVL(DAY_SXF, 0)) AS YEAR_SXF, --Commission
                    SUM(NVL(DAY_FDSY_C, 0))  AS YEAR_FDSY_C ,
                    SUM(NVL(DAY_FDSY_D, 0))  AS YEAR_FDSY_D ,
                    SUM(NVL(DAY_FDSY_A, 0))  AS YEAR_FDSY_A ,
                    SUM(NVL(DAY_PCSY_C, 0)) AS YEAR_PCSY_C, --Close position P/L-Open price as cost
                    SUM(NVL(DAY_PCSY_D, 0)) AS YEAR_PCSY_D, --Close position P/L-Previous settlement price as cost
                    SUM(NVL(DAY_PCSY_A, 0)) AS YEAR_PCSY_A --Close position P/L-Position average price as cost
               FROM XIR_TRD.TTRD_ACC_FUT_BALANCE
              WHERE BASE_DATE <= V_DATE
                AND SUBSTR(BASE_DATE, 1, 7) = SUBSTR(V_DATE, 1, 7) --Current month
              GROUP BY SUBSTR(BASE_DATE, 1, 4),
                       STKID,
                       EXCHID,
                       F_PRODUCTID,
                       EXTMAGCARDID,
                       F_PRODUCTTYPE,
                       BSFLAG,
                       BRANCHID,
                       ACCTID,
                       OFFERREGID) T
      ON (T.BASE_DATE = B.BASE_DATE AND T.BRANCHID = B.BRANCHID AND T.ACCTID = B.ACCTID AND T.OFFERREGID = B.OFFERREGID AND T.STKID = B.STKID AND T.F_PRODUCTID = B.F_PRODUCTID AND T.F_PRODUCTTYPE = B.F_PRODUCTTYPE AND T.EXCHID = B.EXCHID AND T.BSFLAG = B.BSFLAG)
      WHEN MATCHED THEN
        UPDATE
           SET B.YEAR_SXF    = T.YEAR_SXF,
               B.YEAR_SBF    = T.YEAR_SBF,
               B.YEAR_FDSY_C = T.YEAR_FDSY_C,
               B.YEAR_FDSY_D = T.YEAR_FDSY_D,
               B.YEAR_FDSY_A = T.YEAR_FDSY_A,
               B.YEAR_PCSY_C = T.YEAR_PCSY_C,
               B.YEAR_PCSY_D = T.YEAR_PCSY_D,
               B.YEAR_PCSY_A = T.YEAR_PCSY_A
      WHEN NOT MATCHED THEN
        INSERT
          (BASE_DATE,
           BRANCHID,
           ACCTID,
           EXTMAGCARDID,
           OFFERREGID,
           STKID,
           F_PRODUCTID,
           F_PRODUCTTYPE,
           EXCHID,
           BSFLAG,
           YEAR_SBF,
           YEAR_SXF,
           YEAR_FDSY_C,
           YEAR_FDSY_D,
           YEAR_FDSY_A,
           YEAR_PCSY_C,
           YEAR_PCSY_D,
           YEAR_PCSY_A)
        VALUES
          (T.BASE_DATE,
           T.BRANCHID,
           T.ACCTID,
           T.EXTMAGCARDID,
           T.OFFERREGID,
           T.STKID,
           T.F_PRODUCTID,
           T.F_PRODUCTTYPE,
           T.EXCHID,
           T.BSFLAG,
           T.YEAR_SBF,
           T.YEAR_SXF,
           T.YEAR_FDSY_C,
           T.YEAR_FDSY_D,
           T.YEAR_FDSY_A,
           T.YEAR_PCSY_C,
           T.YEAR_PCSY_D,
           T.YEAR_PCSY_A);
      COMMIT;
         /* --Write execution success log:
      INSERT INTO XIR_TRD.TTRD_PROC_RUN_LOG
        (LOGID,BASE_DATE, PROC_NAME, LOG_STATUS, LOG_DESC, UPDATE_CNT, RUN_TIME)
      VALUES
        (NULL,V_DATE, 'PKG_RPT_DAILY_BALANCE_FUT.P_RPT_DAILY_BALANCE_FUT_DETAIL'|| ':Futures per trade P/L landing', '1', 'Execution successful', V_CNT, SYSDATE);
        COMMIT;*/
    END LOOP; --Settlement date range loop

   --Console output
    P_RESULT      := 1;
    P_RESULT_INFO := 'Futures per trade P/L execution details' || 'Start date:' || P_BEG_DATE || 'End date:' ||
                     P_END_DATE || '---' || 'Execution successful';
    --Write execution success log:
    INSERT INTO TTRD_PROC_RUN_LOG
      (PROC_NAME, BASE_DATE, LOG_STATUS, LOG_DESC, UPDATE_CNT)
    VALUES
      (PROC_NAME,
       P_BEG_DATE,
       P_RESULT,
       P_RESULT_INFO,
       V_CNT);  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      --Console output
      P_RESULT      := 0;
      P_RESULT_INFO := 'Futures per trade P/L execution details'|| 'Start date:' || P_BEG_DATE || 'End date:' ||
                       P_END_DATE ||' Location : '||S_MODULE_NAME||SQLCODE || '---' || SQLERRM;
      ROLLBACK;
    --Record execution failure log:
     INSERT INTO TTRD_PROC_RUN_LOG
      (PROC_NAME, BASE_DATE, LOG_STATUS, LOG_DESC, UPDATE_CNT)
    VALUES
      (PROC_NAME,
       P_BEG_DATE,
       P_RESULT,
       P_RESULT_INFO,
       V_CNT);  
      COMMIT;
  END P_RPT_DAILY_BALANCE_FUT_DETAIL;
END PKG_RPT_DAILY_BALANCE_FUT;
/