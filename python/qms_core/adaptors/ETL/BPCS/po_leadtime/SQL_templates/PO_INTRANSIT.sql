WITH 
GI_CTE AS (
    SELECT
        TREF  AS PORD,
        TPROD AS PPROD,
        THLIN AS PLINE,
        TTDTE AS OVERSEA_INVOICE_DATE,
        TQTY  AS INVOICED_QTY
    FROM ITHL02
    WHERE TTYPE = 'GI'
      AND TTDTE > {{ invoice_date_min }}
),
GA_CTE AS (
    SELECT
        TREF  AS PORD,
        TPROD AS PPROD,
        THLIN AS PLINE,
        TTDTE AS OVERSEA_STOCK_IN_DATE,
        TQTY  AS RECEIVED_QTY
    FROM ITHL02
    WHERE TTYPE = 'GA'
      AND TTDTE > {{ invoice_date_min }}
),

OVERSEA AS (
    SELECT
        HPO.PWHSE AS Warehouse,
        HPO.PVEND AS VendorCode,
        HPO.PEDTE AS PO_ENTRY_DATE,
        HPO.PORD AS PONUM,
        HPO.PLINE AS POLINE,
        COALESCE(NULLIF(TRIM(DPO.POTPTC), ''), '53') AS POTPTC,
        TRIM(HPO.PPROD) AS ITEMNUM,
        HPO.PQORD,
        MAX(HPO.PQORD - HPO.PCQTY) AS PQREM,
        COALESCE(SUM(GI.INVOICED_QTY), 0) AS IN_TRANSIT_QTY,
        CHAR(GI.OVERSEA_INVOICE_DATE) AS INVOICE_DATE,
        DPO.POORTC AS ORDER_TYPE,
        HPO.PCMT AS COMMENT
    FROM HPOL02 HPO
    LEFT JOIN GI_CTE GI
        ON HPO.PORD = GI.PORD AND HPO.PPROD = GI.PPROD AND HPO.PLINE = GI.PLINE
    LEFT JOIN GA_CTE GA
        ON HPO.PORD = GA.PORD AND HPO.PPROD = GA.PPROD AND HPO.PLINE = GA.PLINE
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
        AND HPO.PID = 'PO'
        AND HPO.PQORD > HPO.PQREC
        --AND GA.PORD IS NULL
    GROUP BY
        HPO.PWHSE, HPO.PORD, HPO.PLINE, HPO.PPROD,
        HPO.PVEND, GI.OVERSEA_INVOICE_DATE,
        HPO.PQORD, HPO.PEDTE, DPO.POTPTC, DPO.POORTC, HPO.PCMT
),

DOMESTIC AS (
    SELECT
        TRIM(HPO.PWHSE) AS Warehouse,
        HPO.PVEND AS VendorCode,
        HPO.PEDTE AS PO_ENTRY_DATE,
        HPO.PORD AS PONUM,
        HPO.PLINE AS POLINE,
        CAST(COALESCE(DPO.POTPTC, '40') AS CHAR(3)) AS POTPTC,
        TRIM(HPO.PPROD) AS ITEMNUM,
        HPO.PQORD,
        (HPO.PQORD - HPO.PQREC) AS PQREM,
        0 AS IN_TRANSIT_QTY,
        CHAR(HPO.PDDTE) AS INVOICE_DATE,
        COALESCE(DPO.POORTC, 'UR') AS ORDER_TYPE,
        HPO.PCMT AS COMMENT
    FROM HPOL01 HPO
    LEFT JOIN DPOL01 DPO
        ON HPO.PORD = DPO.POPORD
    WHERE
        HPO.PPROD NOT LIKE 'FA%'
        AND HPO.PQORD > HPO.PQREC
        {% if warehouses %}
        AND HPO.PWHSE IN (
            {% for w in warehouses %}
              '{{ w }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        {% endif %}
        AND HPO.PEDTE > {{ po_entry_date_min }}
        AND DPO.POPORD IS NULL
)

SELECT * FROM OVERSEA
UNION ALL
SELECT * FROM DOMESTIC
ORDER BY PONUM, POLINE