CREATE OR REPLACE
PROCEDURE P_GET_ETF_XIR_SONG(P_BEG_DATE VARCHAR2, P_END_DATE VARCHAR2, 
                              R_RETURNDS OUT REDATASET.DS -- results with notable differences
) IS
/* Validate spot P&L differences between Hengtai and Song's ETF market-making data */
V_PROC_NAME VARCHAR2(100):='PKG_DAILY_BALANCE_VALID.P_GET_ETF_XIR_SONG';
BEGIN
    OPEN R_RETURNDS FOR
        WITH XIR AS(-- Hengtai data
            SELECT ETF.BASE_DATE AS BASE_DATE,
                SUM(NVL(SECU_AMOUNT, 0)) AS SECU_AMOUNT, -- Position quantity
                SUM(NVL(SECU_COST, 0)) AS SECU_COST, -- Position cost
                SUM(NVL(SECU_MK, 0)) AS SECU_MK, -- Market value
                SUM(NVL(D_FV, 0)) AS D_FV, -- Today's floating P&L
                SUM(NVL(SECU_TRD, 0) + NVL(ETF_AMT, 0) + NVL(ETF_XJTD, 0)) AS TRD, -- Arbitrage income = trading P&L + ETF creation/redemption value + cash substitution
                SUM(NVL(COMMISION, 0)) AS COMMISION, -- Commission fee
                SUM(NVL(IR, 0)) AS IR, -- Dividend
                SUM(NVL(ETF_XJCE, 0) + NVL(ETF_XJFH, 0)) AS ETF_CASH, -- Cash settlement
                SUM(-NVL(COMMISION, 0) + NVL(ETF_XJFH, 0) + NVL(ETF_XJCE_QD, 0)+ NVL(IR, 0)) AS FJC, -- Non-arbitrage income = cash return + cash compensation + interest/dividend - commission
                SUM(ETF_AMT) as ETF_AMT, -- ETF creation/redemption value
                SUM(ETF_XJTD) as ETF_XJTD, -- Cash substitution
                SUM(ETF_XJCE) as ETF_XJCE, -- Cash difference
                SUM(ETF_XJFH) as ETF_XJFH, -- Cash return
                SUM(ETF_XJCE_QD) as ETF_XJTB, -- Cash compensation
                SUM(ETF.D_PNL+BAL.D_PNL) AS D_PNL -- Today's total P&L
            FROM TTRD_ACC_ETF_BALANCE ETF
            LEFT JOIN (
                SELECT BASE_DATE,
                    bal.CASH_EXT_ACCID,
                    SUM(NVL(DAY_PRFT_FV,0) + NVL(DAY_PRFT_IR,0) + NVL(DAY_PRFT_TRD,0) - NVL(DAY_REAL_FEE,0) - NVL(DAY_AI,0)) AS D_PNL -- Daily P&L
                FROM TRPT_BALANCE_DAILY BAL
                WHERE NVL(YEAR_PRFT_FV,0) + NVL(YEAR_PRFT_IR,0) + NVL(YEAR_PRFT_TRD,0) - NVL(YEAR_REAL_FEE,0) - NVL(YEAR_AI,0)<>0
                    AND BAL.CASH_EXT_ACCID='100000000515' and bal.a_type='SPT_IBOR' 
                    AND BAL.BASE_DATE BETWEEN P_BEG_DATE AND P_END_DATE 
                GROUP BY BASE_DATE, bal.CASH_EXT_ACCID 
            ) BAL
            ON ETF.BASE_DATE=BAL.BASE_DATE
            WHERE ETF.BASE_DATE BETWEEN P_BEG_DATE AND P_END_DATE
            GROUP BY ETF.BASE_DATE
        ),
        SONG AS(-- Song's data
            SELECT NVL(h.QSRQ,t.QSRQ) as QSRQ,
                GPYE2,
                GPJE2,
                FDJE2,
                SYE, -- Arbitrage income
                ETF_XJCE+ETF_XJFH-SXF+ETF_SSXJCE AS FJC, -- Non-arbitrage income (compared to Hengtai: discrepancies usually dividends, counted in SXF field)
                ETF_XJCE+ETF_XJFH-SXF+ETF_SSXJCE+HGJE AS FJC2, -- Song's non-arbitrage income = cash compensation + cash return - commission + ETF cash difference + repo amount
                FDJE2 - LAG(FDJE2,1,0) OVER(ORDER BY NVL(h.QSRQ,t.QSRQ)) AS FV, -- Daily floating P&L
                ETF_CASH,
                ETF_XJCE+ETF_SSXJCE AS ETF_XJTB, -- Cash compensation (vs. Hengtai)
                ETF_XJCE, -- Cash compensation (Song)
                ETF_XJFH,
                SXF, -- Commission
                HGJE, -- Repo amount
                SYE+ETF_XJCE+ETF_XJFH-SXF+ETF_SSXJCE+HGJE+FDJE2 - LAG(FDJE2,1,0) OVER(ORDER BY NVL(h.QSRQ,t.QSRQ)) AS D_PNL -- Today's P&L = arbitrage + non-arbitrage + fair value change
            FROM (
                SELECT TO_CHAR(QSRQ, 'yyyy-mm-dd') AS QSRQ,
                    SUM(GPYE2) AS GPYE2, -- Position quantity
                    SUM(GPJE2) AS GPJE2, -- Position cost
                    SUM(FDJE2) AS FDJE2, -- Market value
                    SUM(SYE) AS SYE -- Arbitrage income
                FROM XIR_ZT.SONG_GPJSKLS -- Song's position data
                WHERE QSRQ BETWEEN DATE P_BEG_DATE AND DATE P_END_DATE           
                    AND ZJZH = '0001100000000515'
                    AND ZQLBMC NOT IN ('债券回购', '特殊业务')
                GROUP BY QSRQ
            ) h
            FULL JOIN (
                SELECT TO_CHAR(QSRQ, 'yyyy-mm-dd') AS QSRQ,
                    SUM(CASE WHEN TRIM(ZYMC)='ETF现金' AND TRIM(REGEXP_REPLACE(BZXX,'[0-9]'))='EFXC-ETF申赎现金差额' AND ZQLBMC NOT IN ('债券回购', '特殊业务') THEN SXF ELSE 0 END) AS ETF_SSXJCE, -- ETF cash difference in creation/redemption
                    SUM(CASE WHEN TRIM(ZYMC)='ETF现金' AND ZQLBMC NOT IN ('债券回购', '特殊业务') THEN SXF ELSE 0 END) AS ETF_CASH, -- Cash settlement
                    SUM(CASE WHEN TRIM(ZYMC)='ETF现金' AND TRIM(REGEXP_REPLACE(BZXX,'[0-9]'))='ETF现金替代、现金差' AND ZQLBMC NOT IN ('债券回购', '特殊业务') THEN SXF ELSE 0 END) AS ETF_XJCE, -- Cash compensation
                    SUM(CASE WHEN TRIM(ZYMC)='ETF现金' AND TRIM(REGEXP_REPLACE(BZXX,'[0-9]'))='ETF补退款-' AND ZQLBMC NOT IN ('债券回购', '特殊业务') THEN SXF ELSE 0 END) AS ETF_XJFH, -- Cash return
                    SUM(CASE WHEN TRIM(ZYMC) IN ('ETF申购','ETF赎回','正常','其它业务') AND ZQLBMC NOT IN ('债券回购', '特殊业务') THEN ABS(SXF) ELSE 0 END) AS SXF, -- Commission
                    SUM(CASE WHEN TRIM(ZYMC)='融券购回' AND TRIM(REGEXP_REPLACE(BZXX,'[0-9]'))='国债账户式回购购回' AND ZQLBMC IN ('债券回购') THEN SXF ELSE 0 END) AS HGJE -- Repo amount
                FROM XIR_ZT.SONG_DZDKLS -- Song's transaction data
                WHERE ZJZH = '0001100000000515' 
                    AND QSRQ BETWEEN DATE P_BEG_DATE AND DATE P_END_DATE
                    AND SXF<>0
                GROUP BY QSRQ
            ) t
            ON h.QSRQ=t.QSRQ 
        )
        SELECT NVL(X.BASE_DATE, S.QSRQ) AS BASE_DATE,
            NVL(X.TRD,0) AS HENGTAI_ARBITRAGE_INCOME,
            NVL(S.SYE,0) AS SONG_ARBITRAGE_INCOME,
            NVL(X.TRD,0)-NVL(S.SYE,0) AS ARBITRAGE_INCOME_DIFF,
            NVL(X.FJC,0) AS HENGTAI_NON_ARBITRAGE_INCOME,
            NVL(S.FJC,0) AS SONG_NON_ARBITRAGE_INCOME,
            NVL(X.FJC,0)-NVL(S.FJC,0) AS NON_ARBITRAGE_INCOME_DIFF,
            NVL(X.D_FV,0) AS HENGTAI_FLOATING_PNL,
            NVL(S.FV,0) AS SONG_FLOATING_PNL,
            NVL(X.D_FV,0)-NVL(S.FV,0) AS FLOATING_PNL_DIFF,
            NVL(X.COMMISION,0) AS HENGTAI_COMMISSION,
            NVL(S.SXF,0) AS SONG_COMMISSION,
            NVL(X.COMMISION,0)-NVL(S.SXF,0) AS COMMISSION_DIFF,
            NVL(X.D_PNL,0) AS HENGTAI_DAILY_PNL,
            NVL(S.D_PNL,0) AS SONG_DAILY_PNL,
            NVL(X.D_PNL,0)-NVL(S.D_PNL,0) AS DAILY_PNL_DIFF
        FROM XIR X
        FULL JOIN SONG S
        ON X.BASE_DATE=S.QSRQ;
END P_GET_ETF_XIR_SONG;
