SELECT  
    TRIM(IWI.WWHS) AS Warehouse,
    TRIM(IWI.WPROD) AS ITEMNUM,
    COALESCE(IWI.WLOTS, 1) AS MOQ,
    COALESCE(IWI.WLEAD, 1) AS WLEAD,
    TRIM(IWI.WILOC) AS WLOC
FROM IWIL01 IWI
INNER JOIN IIML01 IIM ON IWI.WPROD = IIM.IPROD
WHERE IWI.WPROD NOT LIKE 'FA%'
{% if warehouses | length > 0 %}
    AND IWI.WWHS IN (
        {% for wh in warehouses %}
            '{{ wh }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
{% endif %}

{% if location_exclude_rules | length > 0 %}
    {% for rule in location_exclude_rules %}
        AND NOT (IWI.WWHS = '{{ rule.warehouse }}' AND IWI.WILOC = '{{ rule.location }}')
    {% endfor %}
{% endif %}
