CREATE OR REPLACE PROCEDURE P_FUT_GW(P_BEG_DATE    VARCHAR2,
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
    -----------Futures and options data synchronization
    MERGE INTO XIR_TRD.TTRD_SET_BALANCE_SECU_FUT T
    USING (SELECT SUBSTR(C.OCCURTIME, 1, 4) || '-' ||
                  SUBSTR(C.OCCURTIME, 5, 2) || '-' ||
                  SUBSTR(C.OCCURTIME, 7, 2) BEG_DATE,
                  C.REGID ACCID,
                  C.STKID I_CODE,
                  CASE
                    WHEN C.EXCHID = 'X' THEN
                     'XSHG'
                    WHEN C.EXCHID = 'Y' THEN
                     'XSHE'
                    WHEN C.EXCHID = 'F' THEN
                     'X_CNFFEX'
                    WHEN C.EXCHID = 'D' THEN
                     'XDCE'
                    WHEN C.EXCHID = 'S' THEN
                     'XSHG'
                    WHEN C.EXCHID = 'Z' THEN
                     'XZCE'
                    WHEN C.EXCHID = 'N' THEN
                     'XINE'
                  END M_TYPE,
                  CASE
                    WHEN PRODUCTCODE = 'EO' THEN
                     'OPT_F'
                    ELSE
                     'FUT'
                  END A_TYPE,
                  CASE
                    WHEN C.BSFLAG = 'B' THEN
                     'L'
                    WHEN C.BSFLAG = 'S' THEN
                     'S'
                  END LS,
                  C.CURRENTPOSITIONQTY AMOUNT,
                  NVL(C.YDPOSITIONUSABLEQTY, 0) +
                  NVL(C.TODAYPOSITIONUSABLEQTY, 0) AVAAMOUNT,
                  C.YDOFFSFROZPOSITIONQTY UNSALEABLEQTY,
                  C.STKNAME I_NAME,
                  'GW' SEATNO
             FROM ctsdb.vg_futureposition2020@ctssp C) T1
    ON (T.BEG_DATE = T1.BEG_DATE AND T.ACCID = T1.ACCID AND T.I_CODE = T1.I_CODE AND T.M_TYPE = T1.M_TYPE AND T.A_TYPE = T1.A_TYPE AND T.LS = T1.LS)
    WHEN MATCHED THEN
      UPDATE
         SET T.SEATNO        = T1.SEATNO,
             T.AMOUNT        = T1.AMOUNT,
             T.AVAAMOUNT     = T1.AVAAMOUNT,
             T.DATASOURCE    = 'GW',
             T.UPDATETIME    = SYSDATE,
             T.UNSALEABLEQTY = T1.UNSALEABLEQTY,
             T.I_NAME        = T1.I_NAME
    WHEN NOT MATCHED THEN
      INSERT
        (BEG_DATE,
         ACCID,
         I_CODE,
         M_TYPE,
         A_TYPE,
         LS,
         SEATNO,
         AMOUNT,
         AVAAMOUNT,
         DATASOURCE,
         UPDATETIME,
         UNSALEABLEQTY,
         I_NAME)
      VALUES
        (T1.BEG_DATE,
         T1.ACCID,
         T1.I_CODE,
         T1.M_TYPE,
         T1.A_TYPE,
         T1.LS,
         T1.SEATNO,
         T1.AMOUNT,
         T1.AVAAMOUNT,
         'GW',
         SYSDATE,
         T1.UNSALEABLEQTY,
         T1.I_NAME);
    COMMIT;
  
    -----------Stock, fund and bond data synchronization
    MERGE INTO XIR_TRD.TTRD_SET_BALANCE_SECU_FUT T
    USING (SELECT SUBSTR(C.OCCURTIME, 1, 4) || '-' ||
                  SUBSTR(C.OCCURTIME, 5, 2) || '-' ||
                  SUBSTR(C.OCCURTIME, 7, 2) BEG_DATE,
                  C.REGID ACCID,
                  C.STKID I_CODE,
                  CASE
                    WHEN C.EXCHID = '0' THEN
                     'XSHG'
                    WHEN C.EXCHID = '1' THEN
                     'XSHE'
                    WHEN C.EXCHID = '4' THEN
                     '深港通'
                  END M_TYPE,
                  CASE
                    WHEN STKTYPE IN
                         ('A0', 'C8', 'D4', 'D5', 'E1', 'E2', 'E3', 'E4') THEN
                     'SPT_S'
                    WHEN STKTYPE IN ('C4',
                                     'C5',
                                     'A1',
                                     'A2',
                                     'A5',
                                     'B1',
                                     'B2',
                                     'B3',
                                     'B4',
                                     'B5',
                                     'B6',
                                     'C6',
                                     'C7',
                                     'B0',
                                     'B8',
                                     'B9',
                                     'BA',
                                     'BB',
                                     'BC',
                                     'BD',
                                     'BE',
                                     'BF') THEN
                     'SPT_BD'
                    WHEN STKTYPE IN ('A3', 'A8', 'C2') THEN
                     'SPT_F'
                    ELSE
                     'ELSE'
                  END A_TYPE,
                  C.CURRENTQTY AMOUNT,
                  C.USABLEQTY AVAAMOUNT,
                  C.UNSALEABLEQTY,
                  C.STKNAME I_NAME,
                  'GW' SEATNO
             FROM ctsdb.vg_STKLIST2020@ctssp C) T1
    ON (T.BEG_DATE = T1.BEG_DATE AND T.ACCID = T1.ACCID AND T.I_CODE = T1.I_CODE AND T.M_TYPE = T1.M_TYPE /*AND T.A_TYPE = T1.A_TYPE*/
    )
    WHEN MATCHED THEN
      UPDATE
         SET T.SEATNO        = T1.SEATNO,
             T.AMOUNT        = T1.AMOUNT,
             T.AVAAMOUNT     = T1.AVAAMOUNT,
             T.DATASOURCE    = 'GW',
             T.UPDATETIME    = SYSDATE,
             T.UNSALEABLEQTY = T1.UNSALEABLEQTY,
             T.I_NAME        = T1.I_NAME,
             T.LS            = 'L'
      
    
    WHEN NOT MATCHED THEN
      INSERT
        (BEG_DATE,
         ACCID,
         I_CODE,
         M_TYPE,
         A_TYPE,
         LS,
         SEATNO,
         AMOUNT,
         AVAAMOUNT,
         DATASOURCE,
         UPDATETIME,
         UNSALEABLEQTY,
         I_NAME)
      VALUES
        (T1.BEG_DATE,
         T1.ACCID,
         T1.I_CODE,
         T1.M_TYPE,
         T1.A_TYPE,
         'L',
         T1.SEATNO,
         T1.AMOUNT,
         T1.AVAAMOUNT,
         'GW',
         SYSDATE,
         T1.UNSALEABLEQTY,
         T1.I_NAME);
    COMMIT;
  
  END LOOP;
  P_RESULT      := 1;
  P_RESULT_INFO := '根网流水数据落地执行详情' || '开始日期:' || P_BEG_DATE || '结束日期:' ||
                   P_END_DATE || '---' || '执行成功';

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(SQLCODE || '---' || SQLERRM);
  
    P_RESULT      := 0;
    P_RESULT_INFO := '根网流水数据落地执行详情' || '开始日期:' || P_BEG_DATE || '结束日期:' ||
                     P_END_DATE || SQLCODE || '---' || SQLERRM;
  
    ROLLBACK;
END;
/
