SELECT 
            TRIM(ILI.LWHS) AS WAREHOUSE,
            COALESCE(NULLIF(TRIM(ILI.LLOC), ''), 'DEFAULT') AS LOCATION,
            TRIM(ILI.LPROD) AS ITEMNUM,
            TRIM(IIM.IDESC) AS ITEMDESC,
            IIM.ISCST,
            (ILI.LOPB + ILI.LRCT - ILI.LISSU + ILI.LADJU) AS QTYOH,
            (ILI.LOPB + ILI.LRCT - ILI.LISSU + ILI.LADJU - IIM.ICUSA) AS AVAIL,
            ILI.LIALOC,
            IIM.IONOD,
            (ILI.LOPB + ILI.LRCT - ILI.LISSU + ILI.LADJU) * IIM.ISCST AS STOCKVAL,
            COALESCE(NULLIF(TRIM(ILI.LLOT), ''), 'NA') AS SERIAL
        FROM 
            ILIL01 ILI
        INNER JOIN 
            IIML01 IIM ON ILI.LPROD = IIM.IPROD
        WHERE 
            (ILI.LOPB + ILI.LRCT - ILI.LISSU + ILI.LADJU) <> 0
    {% if warehouses | length > 0 %}
        AND ILI.LWHS IN (
            {% for wh in warehouses %}
                '{{ wh }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    {% endif %}