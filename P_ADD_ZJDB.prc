CREATE OR REPLACE PROCEDURE P_ADD_ZJDB(P_BEG_DATE    VARCHAR2,
                                       P_END_DATE    VARCHAR2,
                                       P_RESULT      OUT VARCHAR2,
                                       P_RESULT_INFO OUT VARCHAR2) IS

BEGIN
  FOR CR IN (SELECT TO_CHAR(TO_DATE(P_BEG_DATE, 'YYYY-MM-DD') + ROWNUM - 1,
                            'YYYY-MM-DD') BASEDATE
               FROM DUAL
             CONNECT BY ROWNUM <=
                        TRUNC(TO_DATE(P_END_DATE, 'YYYY-MM-DD') + 1 -
                              TO_DATE(P_BEG_DATE, 'YYYY-MM-DD')))
  LOOP
    -------Fund transfer data loading
    MERGE INTO XIR_TRD_EXH.TTRD_SET_BALANCE_CASH T
    USING (
      WITH CTS AS ---- Genius fund transfer in/out records
       (SELECT ACCTID CASH_EXT_ACCID, ---2019 external fund account
               SUBSTR(OCCURTIME, 1, 4) || '-' || SUBSTR(OCCURTIME, 5, 2) || '-' ||
               SUBSTR(OCCURTIME, 7, 2) SETDATE, ---Record occurrence time
               SUM(NVL(RECKONINGAMT, 0)) CASH_AMOUNT, ----Settlement amount
               SUM(NVL(POSTAMT, 0)) CASH_POSTAMT ----Remaining amount after posting
          FROM VG_tradinglog
         WHERE BRIEFID IN ('002_001_001', '002_002_001') ---Fund deposit/withdrawal
         GROUP BY ACCTID,
                  SUBSTR(OCCURTIME, 1, 4) || '-' || SUBSTR(OCCURTIME, 5, 2) || '-' ||
                  SUBSTR(OCCURTIME, 7, 2))
      
      SELECT C.SETDATE,
             C.CASH_EXT_ACCID,
             NVL(C.CASH_AMOUNT, 0) ZJDB, ---Fund transfer change
             L.ZJDB_SUM_YEAR, ---Year-to-date fund transfer
             C.ZJDB_SUM_ALL ---Total cumulative fund transfer
        FROM (SELECT SETDATE,
                     CASH_EXT_ACCID,
                     CASH_AMOUNT,
                     SUM(NVL(CASH_AMOUNT, 0)) OVER(PARTITION BY CASH_EXT_ACCID ORDER BY SETDATE ASC) ZJDB_SUM_ALL
                FROM CTS) C
        LEFT JOIN (SELECT SETDATE,
                          CASH_EXT_ACCID,
                          SUM(NVL(CASH_AMOUNT, 0)) OVER(PARTITION BY CASH_EXT_ACCID ORDER BY SETDATE ASC) ZJDB_SUM_YEAR
                     FROM CTS
                    WHERE SETDATE >= SUBSTR(CR.BASEDATE, 1, 4) || '-01-01'
                      AND SETDATE <= CR.BASEDATE) L
          ON C.CASH_EXT_ACCID = L.CASH_EXT_ACCID
         AND C.SETDATE = L.SETDATE
       WHERE C.SETDATE = CR.BASEDATE) T1 ON (T.ACCID = T1.CASH_EXT_ACCID AND T.BEG_DATE = T1.SETDATE) WHEN MATCHED THEN
        UPDATE
           SET T.ZJDB          = T1.ZJDB,
               T.ZJDB_SUM_YEAR = T1.ZJDB_SUM_YEAR,
               T.ZJDB_SUM_ALL  = T1.ZJDB_SUM_ALL;
  
    -------Restricted shares, pledged bonds available balance loading
    MERGE INTO XIR_TRD_EXH.TTRD_SET_BALANCE_SECU T
    USING (
      WITH STK AS
       (SELECT SUBSTR(OCCURTIME, 1, 4) || '-' || SUBSTR(OCCURTIME, 5, 2) || '-' ||
               SUBSTR(OCCURTIME, 7, 2) SETDATE, ---Record occurrence time
               REGID AS ACCID, ---Shareholder code
               STKID AS I_CODE,
               CASE
                 WHEN EXCHID IN ('0', '2') THEN
                  'XSHG'
                 WHEN EXCHID IN ('1', '3') THEN
                  'XSHE'
                 WHEN EXCHID IN ('4', '5') THEN
                  'XHKG'
               END AS M_TYPE,
               CASE
                 WHEN STKTYPE IN
                      ('A0', 'C8', 'D4', 'D5', 'E1', 'E2', 'E3', 'E4') THEN
                  'SPT_S'
                 WHEN STKTYPE IN ('C4',
                                  'C5',
                                  'C6',
                                  'C7',
                                  'A1',
                                  'A2',
                                  'A5',
                                  'B1',
                                  'B2',
                                  'B3',
                                  'B4',
                                  'B5',
                                  'B6',
                                  'B0',
                                  'B8',
                                  'B9',
                                  'BA',
                                  'BB',
                                  'BC',
                                  'BD',
                                  'BE',
                                  'BF',
                                  'D3') THEN
                  'SPT_BD'
                 WHEN STKTYPE IN ('A3', 'A8', 'C2') THEN
                  'SPT_F'
                 ELSE
                  'OTHER'
               END AS A_TYPE,
               BONDPLEDGEQTY,
               BONDPLEDGEUSABLEQTY,
               UNSALEABLEQTY, ---2021-04-08 Added non-tradable balance loading
               SELLLIMITQTY ---2021-04-08 Added restricted shares field loading
          FROM VG_STKLIST --2020 data
        ) --2021 data
      
      SELECT SETDATE,
             ACCID,
             I_CODE,
             M_TYPE,
             SUM(BONDPLEDGEQTY) BONDPLEDGEQTY,
             SUM(BONDPLEDGEUSABLEQTY) BONDPLEDGEUSABLEQTY,
             SUM(UNSALEABLEQTY) UNSALEABLEQTY,
             SUM(SELLLIMITQTY) SELLLIMITQTY
        FROM STK
       GROUP BY SETDATE, ACCID, I_CODE, M_TYPE) T1
          ON (T.BEG_DATE = T1.SETDATE AND T.ACCID = T1.ACCID AND
             T.I_CODE = T1.I_CODE AND T.M_TYPE = T1.M_TYPE) WHEN
       MATCHED THEN
        UPDATE
           SET T.BONDPLEDGEQTY       = T1.BONDPLEDGEQTY,
               T.BONDPLEDGEUSABLEQTY = T1.BONDPLEDGEUSABLEQTY,
               --T.UNSALEABLEQTY       = T1.UNSALEABLEQTY,
               T.SELLLIMITQTY = T1.SELLLIMITQTY;
  
  END LOOP;
  P_RESULT      := 1;
  P_RESULT_INFO := 'Fund transfer records, restricted shares balance, pledged bonds available balance loading task executed successfully' || 'Start date:' || P_BEG_DATE ||
                   'End date:' || P_END_DATE;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    P_RESULT      := 0;
    P_RESULT_INFO := 'Fund transfer records, restricted shares balance, pledged bonds available balance loading task execution failed' || 'Start date:' ||
                     P_BEG_DATE || 'End date:' || P_END_DATE || SQLCODE ||
                     '---' || SQLERRM;
    ROLLBACK;
  
END;
/