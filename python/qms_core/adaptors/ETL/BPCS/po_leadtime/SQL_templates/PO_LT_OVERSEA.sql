-- templates/PO_LT_OVERSEA.sql

WITH  
GI_CTE AS (
    SELECT
        TREF    AS PORD,
        TPROD   AS PPROD,
        THLIN   AS PLINE,
        TTDTE   AS OVERSEA_INVOICE_DATE,
        TQTY    AS INVOICED_QTY,
        ROW_NUMBER() OVER (
            PARTITION BY TREF, TPROD, THLIN, TQTY
            ORDER BY TSEQ
        ) AS rn_qty
    FROM ITHL02
    WHERE TTYPE = 'GI'
      AND TTDTE > {{ invoice_date_min }}
),
GA_CTE AS (
    SELECT
        TREF    AS PORD,
        TPROD   AS PPROD,
        THLIN   AS PLINE,
        TTDTE   AS OVERSEA_STOCK_IN_DATE,
        TQTY    AS RECEIVED_QTY,
        ROW_NUMBER() OVER (
            PARTITION BY TREF, TPROD, THLIN, TQTY
            ORDER BY TSEQ
        ) AS rn_qty
    FROM ITHL02
    WHERE TTYPE = 'GA'
      AND TTDTE > {{ stockin_date_min }}
)

SELECT
    HPO.PWHSE,
    HPO.PORD,
    HPO.PLINE,
    TRIM(HPO.PPROD)                         AS PPROD,
    HPO.PVEND,
    MAX(HPO.PQORD)                          AS PQORD,
    CHAR(HPO.PEDTE)                         AS PO_ENTRY_DATE,
    CHAR(GI.OVERSEA_INVOICE_DATE)          AS OVERSEA_INVOICE_DATE,
    CHAR(GA.OVERSEA_STOCK_IN_DATE)         AS OVERSEA_STOCK_IN_DATE,
    SUM(GI.INVOICED_QTY)                   AS INVOICED_QTY,
    SUM(GA.RECEIVED_QTY)                   AS RECEIVED_QTY,
    COALESCE(NULLIF(TRIM(DPO.POTPTC), ''), '53') AS POTPTC,
    DPO.POORTC                              AS ORDER_TYPE,
    CASE
        WHEN HPO.PQORD = HPO.PQREC THEN 'Y'
        ELSE 'N'
    END AS IS_PO_CLOSED

FROM HPOL02 HPO

INNER JOIN GI_CTE GI
    ON HPO.PORD = GI.PORD
    AND HPO.PPROD = GI.PPROD
    AND HPO.PLINE = GI.PLINE

INNER JOIN GA_CTE GA
    ON HPO.PORD = GA.PORD
    AND HPO.PPROD = GA.PPROD
    AND HPO.PLINE = GA.PLINE
    AND GA.rn_qty = GI.rn_qty
    AND GA.RECEIVED_QTY = GI.INVOICED_QTY

INNER JOIN DPOL02 DPO
    ON HPO.PORD = DPO.POPORD

WHERE
    HPO.PEDTE > {{ po_entry_date_min }}
    {% if warehouses %}
      AND HPO.PWHSE IN (
        {% for w in warehouses %}
          '{{ w }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
      )
    {% endif %}

GROUP BY
    HPO.PWHSE,
    HPO.PORD,
    HPO.PLINE,
    HPO.PPROD,
    HPO.PVEND,
    HPO.PEDTE,
    GI.OVERSEA_INVOICE_DATE,
    GA.OVERSEA_STOCK_IN_DATE,
    DPO.POTPTC,
    DPO.POORTC,
    CASE
        WHEN HPO.PQORD = HPO.PQREC THEN 'Y'
        ELSE 'N'
    END

ORDER BY 
    HPO.PORD,
    HPO.PLINE,
    GI.OVERSEA_INVOICE_DATE
