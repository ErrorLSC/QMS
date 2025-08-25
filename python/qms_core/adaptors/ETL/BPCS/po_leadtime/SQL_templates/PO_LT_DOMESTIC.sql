SELECT *
FROM (
    SELECT 
        HPO.PWHSE,
        HPO.PORD,
        HPO.PLINE,
        TRIM(HPO.PPROD) AS PPROD,
        HPO.PVEND,
        HPO.PQORD,
        HPO.PQREC,
        HPO.PEDTE AS PO_ENTRY_DATE,
        HPO.HVDUE AS CONFIRMED_DELIVERY_DATE,
        H.TTDTE  AS LOCAL_STOCK_IN_DATE,
        H.LOT_NUMBER,

        CASE 
            WHEN HPO.PQORD = HPO.PQREC THEN 'Y'
            ELSE 'N'
        END AS IS_PO_CLOSED,

        ROW_NUMBER() OVER (
            PARTITION BY HPO.PORD, TRIM(HPO.PPROD)
            ORDER BY H.TTDTE
        ) AS RN

    FROM HPO

    INNER JOIN (
        SELECT 
            TREF, TPROD, TTDTE, TLOT AS LOT_NUMBER
        FROM (
            SELECT 
                TREF, TPROD, TTDTE, TLOT,
                ROW_NUMBER() OVER (PARTITION BY TREF, TPROD ORDER BY TTDTE ASC) AS RN
            FROM ITHL02
            WHERE TTYPE = 'H'
              AND TTDTE > {{ stockin_date_min }}
        ) AS H1
        WHERE H1.RN = 1
    ) H
    ON HPO.PORD = H.TREF  
    AND HPO.PPROD = H.TPROD  

    WHERE HPO.PEDTE > {{ po_entry_date_min }}
    {% if enable_hotzone %}
      AND H.TTDTE >= CAST(TO_CHAR(CURRENT DATE - 6 MONTHS, 'YYYYMMDD') AS INTEGER)
    {% endif %}
    {% if warehouses %}
      AND HPO.PWHSE IN (
        {% for w in warehouses %}
          '{{ w }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
      )
    {% endif %}
    AND HPO.PVEND > 10000
    AND HPO.PQORD = HPO.PQREC
) Z
WHERE Z.RN = 1
