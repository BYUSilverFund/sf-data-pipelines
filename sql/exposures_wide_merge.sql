INSERT INTO exposures_wide
(
    date,
    barrid,
    beta ,
    country,
    divyild,
    earnqlty,
    earnyild,
    growth,
    leverage,
    liquidty,
    ltrevrsl,
    mgmtqlty,
    midcap,
    momentum,
    profit,
    prospect,
    resvol,
    size,
    value,
    aerodef,
    airlines,
    alumstel,
    apparel,
    auto,
    banks,
    bevtob,
    biolife,
    bldgprod,
    chem,
    cnsteng,
    cnstmach,
    cnstmatl,
    commeqp,
    compelec,
    comsvcs,
    conglom,
    containr,
    distrib,
    divfin,
    eleceqp,
    elecutil,
    foodprod,
    foodret,
    gasutil,
    hltheqp,
    hlthsvcs,
    homebldg,
    housedur,
    indmach,
    insurnce,
    internet,
    leisprod,
    leissvcs,
    lifeins,
    media,
    mgdhlth,
    multutil,
    oilgscon,
    oilgsdrl,
    oilgseqp,
    oilgsexp,
    paper,
    pharma,
    precmtls,
    psnlprod,
    realest,
    restaur,
    roadrail,
    semicond,
    semieqp,
    software,
    spltyret,
    sptychem,
    sptystor,
    telecom,
    tradeco,
    transprt,
    wireless
)
SELECT 
    date,
    barrid,
    USSLOWL_BETA AS beta,
    USSLOWL_COUNTRY AS country,
    USSLOWL_DIVYILD AS divyild,
    USSLOWL_EARNQLTY AS earnqlty,
    USSLOWL_EARNYILD AS earnyild,
    USSLOWL_GROWTH AS growth,
    USSLOWL_LEVERAGE AS leverage,
    USSLOWL_LIQUIDTY AS liquidty,
    USSLOWL_LTREVRSL AS ltrevrsl,
    USSLOWL_MGMTQLTY AS mgmtqlty,
    USSLOWL_MIDCAP AS midcap,
    USSLOWL_MOMENTUM AS momentum,
    USSLOWL_PROFIT AS profit,
    USSLOWL_PROSPECT AS prospect,
    USSLOWL_RESVOL AS resvol,
    USSLOWL_SIZE AS size,
    USSLOWL_VALUE AS value,
    USSLOWL_AERODEF AS aerodef,
    USSLOWL_AIRLINES AS airlines,
    USSLOWL_ALUMSTEL AS alumstel,
    USSLOWL_APPAREL AS apparel,
    USSLOWL_AUTO AS auto,
    USSLOWL_BANKS AS banks,
    USSLOWL_BEVTOB AS bevtob,
    USSLOWL_BIOLIFE AS biolife,
    USSLOWL_BLDGPROD AS bldgprod,
    USSLOWL_CHEM AS chem,
    USSLOWL_CNSTENG AS cnsteng,
    USSLOWL_CNSTMACH AS cnstmach,
    USSLOWL_CNSTMATL AS cnstmatl,
    USSLOWL_COMMEQP AS commeqp,
    USSLOWL_COMPELEC AS compelec,
    USSLOWL_COMSVCS AS comsvcs,
    USSLOWL_CONGLOM AS conglom,
    USSLOWL_CONTAINR AS containr,
    USSLOWL_DISTRIB AS distrib,
    USSLOWL_DIVFIN AS divfin,
    USSLOWL_ELECEQP AS eleceqp,
    USSLOWL_ELECUTIL AS elecutil,
    USSLOWL_FOODPROD AS foodprod,
    USSLOWL_FOODRET AS foodret,
    USSLOWL_GASUTIL AS gasutil,
    USSLOWL_HLTHEQP AS hltheqp,
    USSLOWL_HLTHSVCS AS hlthsvcs,
    USSLOWL_HOMEBLDG AS homebldg,
    USSLOWL_HOUSEDUR AS housedur,
    USSLOWL_INDMACH AS indmach,
    USSLOWL_INSURNCE AS insurnce,
    USSLOWL_INTERNET AS internet,
    USSLOWL_LEISPROD AS leisprod,
    USSLOWL_LEISSVCS AS leissvcs,
    USSLOWL_LIFEINS AS lifeins,
    USSLOWL_MEDIA AS media,
    USSLOWL_MGDHLTH AS mgdhlth,
    USSLOWL_MULTUTIL AS multutil,
    USSLOWL_OILGSCON AS oilgscon,
    USSLOWL_OILGSDRL AS oilgsdrl,
    USSLOWL_OILGSEQP AS oilgseqp,
    USSLOWL_OILGSEXP AS oilgsexp,
    USSLOWL_PAPER AS paper,
    USSLOWL_PHARMA AS pharma,
    USSLOWL_PRECMTLS AS precmtls,
    USSLOWL_PSNLPROD AS psnlprod,
    USSLOWL_REALEST AS realest,
    USSLOWL_RESTAUR AS restaur,
    USSLOWL_ROADRAIL AS roadrail,
    USSLOWL_SEMICOND AS semicond,
    USSLOWL_SEMIEQP AS semieqp,
    USSLOWL_SOFTWARE AS software,
    USSLOWL_SPLTYRET AS spltyret,
    USSLOWL_SPTYCHEM AS sptychem,
    USSLOWL_SPTYSTOR AS sptystor,
    USSLOWL_TELECOM AS telecom,
    USSLOWL_TRADECO AS tradeco,
    USSLOWL_TRANSPRT AS transprt,
    USSLOWL_WIRELESS AS wireless
FROM {{ source_table }} AS s
ON CONFLICT (date, barrid) DO UPDATE SET 
    beta = excluded.beta,
    country = excluded.country,
    divyild = excluded.divyild,
    earnqlty = excluded.earnqlty,
    earnyild = excluded.earnyild,
    growth = excluded.growth,
    leverage = excluded.leverage,
    liquidty = excluded.liquidty,
    ltrevrsl = excluded.ltrevrsl,
    mgmtqlty = excluded.mgmtqlty,
    midcap = excluded.midcap,
    momentum = excluded.momentum,
    profit = excluded.profit,
    prospect = excluded.prospect,
    resvol = excluded.resvol,
    size = excluded.size,
    value = excluded.value,
    aerodef = excluded.aerodef,
    airlines = excluded.airlines,
    alumstel = excluded.alumstel,
    apparel = excluded.apparel,
    auto = excluded.auto,
    banks = excluded.banks,
    bevtob = excluded.bevtob,
    biolife = excluded.biolife,
    bldgprod = excluded.bldgprod,
    chem = excluded.chem,
    cnsteng = excluded.cnsteng,
    cnstmach = excluded.cnstmach,
    cnstmatl = excluded.cnstmatl,
    commeqp = excluded.commeqp,
    compelec = excluded.compelec,
    comsvcs = excluded.comsvcs,
    conglom = excluded.conglom,
    containr = excluded.containr,
    distrib = excluded.distrib,
    divfin = excluded.divfin,
    eleceqp = excluded.eleceqp,
    elecutil = excluded.elecutil,
    foodprod = excluded.foodprod,
    foodret = excluded.foodret,
    gasutil = excluded.gasutil,
    hltheqp = excluded.hltheqp,
    hlthsvcs = excluded.hlthsvcs,
    homebldg = excluded.homebldg,
    housedur = excluded.housedur,
    indmach = excluded.indmach,
    insurnce = excluded.insurnce,
    internet = excluded.internet,
    leisprod = excluded.leisprod,
    leissvcs = excluded.leissvcs,
    lifeins = excluded.lifeins,
    media = excluded.media,
    mgdhlth = excluded.mgdhlth,
    multutil = excluded.multutil,
    oilgscon = excluded.oilgscon,
    oilgsdrl = excluded.oilgsdrl,
    oilgseqp = excluded.oilgseqp,
    oilgsexp = excluded.oilgsexp,
    paper = excluded.paper,
    pharma = excluded.pharma,
    precmtls = excluded.precmtls,
    psnlprod = excluded.psnlprod,
    realest = excluded.realest,
    restaur = excluded.restaur,
    roadrail = excluded.roadrail,
    semicond = excluded.semicond,
    semieqp = excluded.semieqp,
    software = excluded.software,
    spltyret = excluded.spltyret,
    sptychem = excluded.sptychem,
    sptystor = excluded.sptystor,
    telecom = excluded.telecom,
    tradeco = excluded.tradeco,
    transprt = excluded.transprt,
    wireless = excluded.wireless
;
