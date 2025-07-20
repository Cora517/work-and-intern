CREATE OR REPLACE
PROCEDURE P_GET_ETF_XIR_SONG(P_BEG_DATE VARCHAR2,P_END_DATE VARCHAR2, 
                                                       R_RETURNDS OUT REDATASET.DS--Results with differences
) IS
/*Validate ETF market making profit/loss between Hengtai and Teacher Song's spot*/
V_PROC_NAME VARCHAR2(100):='PKG_DAILY_BALANCE_VALID.P_GET_ETF_XIR_SONG';
BEGIN
	OPEN R_RETURNDS FOR
	     WITH XIR AS(--Hengtai data
			      SELECT ETF.BASE_DATE AS BASE_DATE,
               SUM(NVL(SECU_AMOUNT, 0)) AS SECU_AMOUNT,--Position quantity
               SUM(NVL(SECU_COST, 0)) AS SECU_COST,--Position cost
               SUM(NVL(SECU_MK, 0)) AS SECU_MK,--Market value
               SUM(NVL(D_FV, 0)) AS D_FV,--Today's floating profit/loss
               SUM(NVL(SECU_TRD, 0) + NVL(ETF_AMT, 0) + NVL(ETF_XJTD, 0)) AS TRD,--Spread income = trading profit/loss + subscription/redemption market value + cash substitution
               SUM(NVL(COMMISION, 0)) AS COMMISION,--Commission
               SUM(NVL(IR, 0)) AS IR,--Dividend
               SUM(NVL(ETF_XJCE, 0) + NVL(ETF_XJFH, 0)) AS ETF_CASH,--Cash settlement
							 SUM(-NVL(COMMISION, 0) + NVL(ETF_XJFH, 0) + NVL(ETF_XJCE_QD, 0)+ NVL(IR, 0)) AS FJC,--Non-spread income = cash return + cash refund + interest dividend - commission
               SUM(ETF_AMT) as ETF_AMT,--Subscription/redemption market value
               SUM(ETF_XJTD) as ETF_XJTD,--Cash substitution
               SUM(ETF_XJCE) as ETF_XJCE,--Cash difference
               SUM(ETF_XJFH) as ETF_XJFH,--Cash return
							 SUM(ETF_XJCE_QD) as ETF_XJTB,--Cash refund
               SUM(ETF.D_PNL+BAL.D_PNL) AS D_PNL--Today's profit/loss
	           FROM TTRD_ACC_ETF_BALANCE ETF
						 LEFT JOIN (
						 SELECT  BASE_DATE,
              bal.CASH_EXT_ACCID,
      SUM(NVL(DAY_PRFT_FV,0)  +NVL(DAY_PRFT_IR,0)  +NVL(DAY_PRFT_TRD,0)  -NVL(DAY_REAL_FEE,0)  -NVL(DAY_AI,0))   AS D_PNL--Daily profit/loss
      /*SUM(NVL(MONTH_PRFT_FV,0)+NVL(MONTH_PRFT_IR,0)+NVL(MONTH_PRFT_TRD,0)-NVL(MONTH_REAL_FEE,0)-NVL(MONTH_AI,0)) AS M_PNL,--Monthly profit/loss
      SUM(NVL(YEAR_PRFT_FV,0) +NVL(YEAR_PRFT_IR,0) +NVL(YEAR_PRFT_TRD,0) -NVL(YEAR_REAL_FEE,0) -NVL(YEAR_AI,0))  AS Y_PNL--Yearly profit/loss*/
      FROM TRPT_BALANCE_DAILY BAL
      WHERE NVL(YEAR_PRFT_FV,0) +NVL(YEAR_PRFT_IR,0) +NVL(YEAR_PRFT_TRD,0) -NVL(YEAR_REAL_FEE,0) -NVL(YEAR_AI,0)<>0
      AND BAL.CASH_EXT_ACCID='100000000515' and bal.a_type='SPT_IBOR' 
      AND BAL.BASE_DATE BETWEEN P_BEG_DATE AND P_END_DATE 
      GROUP BY BASE_DATE,
               bal.CASH_EXT_ACCID 
						 )BAL
						 ON ETF.BASE_DATE=BAL.BASE_DATE
						 WHERE ETF.BASE_DATE BETWEEN P_BEG_DATE AND P_END_DATE
             GROUP BY ETF.BASE_DATE
						 ),
			      SONG AS(--Teacher Song's data
						   SELECT nvl(h.QSRQ,t.QSRQ) as QSRQ,
                      GPYE2,
											GPJE2,
											FDJE2,
											SYE,--Spread income
											ETF_XJCE+ETF_XJFH-SXF+ETF_SSXJCE AS FJC,--Non-spread income (compared with Hengtai: if there's a difference, it's dividend, included in SXF field)
											ETF_XJCE+ETF_XJFH-SXF+ETF_SSXJCE+HGJE AS FJC2,--Non-spread income (Song) = cash refund + cash return - commission + ETF subscription/redemption cash difference + repurchase amount
											FDJE2-LAG( FDJE2,1,0) OVER( ORDER BY nvl(h.QSRQ,t.QSRQ) ASC) AS FV,--Daily floating profit/loss
                      ETF_CASH,
                      ETF_XJCE+ETF_SSXJCE AS ETF_XJTB,--Cash refund (compared with Hengtai)
											ETF_XJCE,--Cash refund (Song)
                      ETF_XJFH,
                      SXF,--Commission
											HGJE,--Repurchase amount
								      SYE+ETF_XJCE+ETF_XJFH-SXF+ETF_SSXJCE+HGJE+FDJE2-LAG( FDJE2,1,0) OVER( ORDER BY nvl(h.QSRQ,t.QSRQ) ASC) AS D_PNL--Daily profit/loss = spread income + non-spread + fair value change
             FROM(
             SELECT TO_CHAR(QSRQ, 'yyyy-mm-dd') AS QSRQ,
                    SUM(GPYE2) AS GPYE2,--Position quantity
                    SUM(GPJE2) AS GPJE2,--Position cost
                    SUM(FDJE2) AS FDJE2,--Market value
                    SUM(SYE) AS SYE--Spread income
               FROM XIR_ZT.SONG_GPJSKLS --Teacher Song's position
              WHERE QSRQ BETWEEN DATE P_BEG_DATE AND DATE P_END_DATE           
                AND ZJZH = '0001100000000515'
                AND ZQLBMC NOT IN ('Bond repurchase', 'Special business')
                GROUP BY QSRQ)h
              FULL JOIN (
              SELECT TO_CHAR(QSRQ, 'yyyy-mm-dd') as QSRQ,
                     SUM(CASE WHEN TRIM(ZYMC)='ETF Cash' AND TRIM(regexp_replace(BZXX,'[0-9]'))='EFXC-ETF Subscription/Redemption Cash Difference' AND ZQLBMC NOT IN ('Bond repurchase', 'Special business')THEN SXF ELSE 0 END) AS ETF_SSXJCE,--ETF subscription/redemption cash difference
                     SUM(CASE WHEN TRIM(ZYMC)='ETF Cash' AND ZQLBMC NOT IN ('Bond repurchase', 'Special business')THEN sxf ELSE 0 END ) AS etf_cash,--Cash settlement
                     SUM(CASE WHEN TRIM(ZYMC)='ETF Cash' AND TRIM(regexp_replace(BZXX,'[0-9]'))='ETF Cash Substitution, Cash Difference' AND ZQLBMC NOT IN ('Bond repurchase', 'Special business')THEN SXF ELSE 0 END ) AS ETF_XJCE,--Cash refund
                     SUM(CASE WHEN TRIM(ZYMC)='ETF Cash' AND TRIM(regexp_replace(BZXX,'[0-9]'))='ETF Refund-' AND ZQLBMC NOT IN ('Bond repurchase', 'Special business')THEN SXF ELSE 0 END ) as etf_xjfh,--Cash return
                     SUM(CASE WHEN TRIM(ZYMC) IN ('ETF Subscription','ETF Redemption','Normal','Other business') AND ZQLBMC NOT IN ('Bond repurchase', 'Special business')THEN ABS(SXF) ELSE 0 END) as SXF,--Commission
										 SUM(CASE WHEN TRIM(ZYMC)='Securities lending repurchase' AND TRIM(regexp_replace(BZXX,'[0-9]'))='Treasury account repurchase' AND ZQLBMC IN ('Bond repurchase') THEN SXF ELSE 0 END) AS HGJE--Repurchase amount
                FROM xir_zt.song_dzdkls --Teacher Song's transactions
               WHERE ZJZH = '0001100000000515' 
							 AND QSRQ BETWEEN DATE P_BEG_DATE AND DATE P_END_DATE
               AND SXF<>0
               GROUP BY QSRQ
              )t
              on h.QSRQ=t.QSRQ 
							)
							
				      SELECT NVL(X.BASE_DATE,S.QSRQ) AS BASE_DATE,
							       NVL(X.TRD,0) AS Hengtai Spread Income,
										 NVL(S.SYE,0) AS Song Spread Income,
							       NVL(X.TRD,0)-NVL(S.SYE,0) AS  Spread Income Difference,
										 NVL(X.FJC,0) AS Hengtai Non-spread Income,
										 NVL(S.FJC,0) AS Song Non-spread Income,
										 NVL(X.FJC,0)-NVL(S.FJC,0) AS Non-spread Income Difference,
										 NVL(X.D_FV,0) AS Hengtai Floating Profit/Loss,
										 NVL(S.FV,0) AS Song Floating Profit/Loss,
										 NVL(X.D_FV,0)-NVL(S.FV,0) AS Floating Profit/Loss Difference,
										 NVL(X.COMMISION,0) AS Hengtai Commission,
										 NVL(S.SXF,0) AS Song Commission,
										 NVL(X.COMMISION,0)-NVL(S.SXF,0) AS Commission Difference,
										 NVL(X.D_PNL,0) AS Hengtai Daily Profit/Loss,
										 NVL(S.D_PNL,0) AS Song Daily Profit/Loss,
										 NVL(X.D_PNL,0)-NVL(S.D_PNL,0) AS Daily Profit/Loss Difference
							FROM XIR X
							FULL JOIN SONG S
							ON X.BASE_DATE=S.QSRQ			 
								;
END P_GET_ETF_XIR_SONG;
