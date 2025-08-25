SELECT 
    IIM.IITYP,
    TRIM(IIM.IPROD) AS ITEMNUM,
    TRIM(IIM.IDESC) AS IDESC,
    TRIM(IIM.IDSCE) AS IDSCE,
    TRIM(CHAR(IIM.IVEND)) AS IVEND,
    TRIM(AVM.VNDNAM) AS VNDNAM,
    IIM.ISCST,
    TRIM(ICX.CXPPLC) AS CXPPLC,
    TRIM(ICX.CXATLC) AS PGC,
    TRIM(ICX.CXPGAC) AS GAC,
    COALESCE(IIX.IXNWIG, 0) / 1000 AS NETWEIGHT_KG
FROM IIML01 IIM
LEFT JOIN ICXL01 ICX ON IIM.ICLAS = ICX.CXCLAS
INNER JOIN IIXL01 IIX ON IIM.IPROD = IIX.IXITMN
INNER JOIN AVML01 AVM ON IIM.IVEND = AVM.VENDOR
WHERE IIM.IPROD NOT LIKE 'FA%'
  AND IIM.IVEND > 101
{% if item_types %}
  AND IIM.IITYP IN (
    {% for t in item_types %}
      '{{ t }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
  )
{% endif %}
{% if plcs %}
  AND ICX.CXPPLC IN (
    {% for l in plcs %}
      '{{ l }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
  )
{% endif %}
