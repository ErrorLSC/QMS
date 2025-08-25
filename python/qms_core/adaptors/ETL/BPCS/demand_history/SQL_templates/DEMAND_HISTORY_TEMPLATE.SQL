SELECT   
    H.HDTYP,
    H.HCUST,
    L.LODTE,
    L.LORD,
    L.LLINE,
    TRIM(L.LPROD) AS ITEMNUM,
    L.LQORD,
    L.LRDTE,
    L.LSDTE,
    TRIM(L.LWHS) AS WAREHOUSE,
    JSB.SBSWKT
FROM JPNPRDF.ECL L
INNER JOIN JPNPRDF.ECH H ON L.LORD = H.HORD
INNER JOIN JPNPRDF.IIM IIM ON L.LPROD = IIM.IPROD
LEFT JOIN JSB ON H.HORD = JSB.SBHORD
WHERE 
    L.LODTE > {{ min_order_entry_date }}
{% if only_recent_orders %}
    AND L.LODTE >= CAST(TO_CHAR(CURRENT DATE - 6 MONTHS, 'YYYYMMDD') AS INTEGER)
{% endif %}
    AND L.LPROD NOT LIKE 'FA%'
{% if allowed_item_types | length > 0 %}
    AND IIM.IITYP IN (
        {% for t in allowed_item_types %}
        '{{ t }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
{% endif %}
{% if exclude_deleted_orders %}
    AND NOT ((H.HID = 'CZ' OR L.LID = 'CZ') AND L.LQSHP = 0)
{% endif %}
{% if allowed_work_types | length > 0 %}
    AND (JSB.SBSWKT IS NULL OR JSB.SBSWKT IN (
        {% for w in allowed_work_types %}
        '{{ w }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    ))
{% else %}
    AND JSB.SBSWKT IS NULL
{% endif %}
{% if warehouses | length > 0 %}
    AND TRIM(L.LWHS) IN (
        {% for w in warehouses %}
        '{{ w }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
{% endif %}
    AND NOT (JSB.SBSWKT IN ('ASY', 'KIT') AND L.LQORD < 0)
{% if exclude_redistribution_orders %}
    AND NOT (H.HCUST > 1000 AND H.HCUST < 10000 AND H.HCUST != 9125)
{% endif %}
{%if exclude_internal_orders %}
    AND H.HCUST > 1000
{% endif %}