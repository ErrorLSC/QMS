SELECT 
    TRIM(HPO.PWHSE) AS Warehouse,
    HPO.PVEND as VendorCode,HPO.PEDTE as POEntryDate,
    HPO.PORD as PONUM,
    HPO.PLINE as POLINE,
    COALESCE(DPO.POTPTC, 40) as POTPTC,
    TRIM(HPO.PPROD) AS ITEMNUM,
    HPO.PQORD,HPO.PQREC,(HPO.PQORD-HPO.PQREC) as PQREM,
    HPO.PCQTY,(HPO.PCQTY-HPO.PQREC)as PQTRANSIT,
    HPO.PDDTE as DueDate,HPO.HVDUE as DeliveryDate,
    HPO.PCMT
FROM HPOL01 HPO
LEFT JOIN DPOL01 DPO ON HPO.PORD=DPO.POPORD
WHERE HPO.PEDTE > 20001101
    AND HPO.PPROD NOT LIKE 'FA%'
    AND HPO.PQORD > HPO.PQREC
{% if warehouses %}
  AND HPO.PWHSE IN (
    {% for warehouse in warehouses %}
      '{{ warehouse }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
  )
{% endif %}
ORDER BY HPO.PEDTE