SELECT  
    TRIM(WWHS) AS WWHS,
    TRIM(WLOC) AS WLOC,
    TRIM(WDESC) AS WDESC,
    WLTYP,
    WZONE,
    WVOLC AS VOLCAP,
    WWGHC AS WEIGHTCAP
FROM ILML01
{% if warehouses %}
WHERE WWHS IN (
    {% for wh in warehouses %}
        '{{ wh }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
)
{% endif %}