SELECT TRIM(IWI.WWHS) AS WAREHOUSE,
		TRIM(IWI.WPROD) AS ITEMNUM,
		TRIM(IIM.IDESC) AS ITEMDESC,IIM.ISCST,
		(IWI.WOPB + IWI.WRCT - IWI.WISS + IWI.WADJ) AS QTYOH,
		(IWI.WOPB + IWI.WRCT - IWI.WISS + IWI.WADJ-IWI.WCUSA) AS AVAIL,
		IIM.IONOD,
		(IWI.WOPB + IWI.WRCT - IWI.WISS + IWI.WADJ)*IIM.ISCST AS STOCKVAL
    FROM IWIL01 IWI
    INNER JOIN IIML01 IIM ON IWI.WPROD = IIM.IPROD
    WHERE ((IWI.WOPB + IWI.WRCT - IWI.WISS + IWI.WADJ) <> 0 OR IWI.WCUSA <> 0 OR IIM.IONOD > 0)
    {% if warehouses | length > 0 %}
        AND IWI.WWHS IN (
            {% for wh in warehouses %}
                '{{ wh }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
    {% endif %}