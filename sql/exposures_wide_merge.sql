INSERT INTO exposures_wide
(
    date,
    barrid,
    USSLOWL_BETA,
    USSLOWL_COUNTRY,
    USSLOWL_DIVYILD,
    USSLOWL_EARNQLTY,
    USSLOWL_EARNYILD,
    USSLOWL_GROWTH,
    USSLOWL_LEVERAGE,
    USSLOWL_LIQUIDTY,
    USSLOWL_LTREVRSL,
    USSLOWL_MGMTQLTY,
    USSLOWL_MIDCAP,
    USSLOWL_MOMENTUM,
    USSLOWL_PROFIT,
    USSLOWL_PROSPECT,
    USSLOWL_SIZE,
    USSLOWL_VALUE,
    USSLOWL_AERODEF,
    USSLOWL_AIRLINES,
    USSLOWL_ALUMSTEL,
    USSLOWL_APPAREL,
    USSLOWL_AUTO,
    USSLOWL_BANKS,
    USSLOWL_BEVTOB,
    USSLOWL_BIOLIFE,
    USSLOWL_BLDGPROD,
    USSLOWL_CHEM,
    USSLOWL_CNSTENG,
    USSLOWL_CNSTMACH,
    USSLOWL_CNSTMATL,
    USSLOWL_COMMEQP,
    USSLOWL_COMPELEC,
    USSLOWL_COMSVCS,
    USSLOWL_CONGLOM,
    USSLOWL_CONTAINR,
    USSLOWL_DISTRIB,
    USSLOWL_DIVFIN,
    USSLOWL_ELECEQP,
    USSLOWL_ELECUTIL,
    USSLOWL_FOODPROD,
    USSLOWL_FOODRET,
    USSLOWL_GASUTIL,
    USSLOWL_HLTHEQP,
    USSLOWL_HLTHSVCS,
    USSLOWL_HOMEBLDG,
    USSLOWL_HOUSEDUR,
    USSLOWL_INDMACH,
    USSLOWL_INSURNCE,
    USSLOWL_INTERNET,
    USSLOWL_LEISPROD,
    USSLOWL_LEISSVCS,
    USSLOWL_LIFEINS,
    USSLOWL_MEDIA,
    USSLOWL_MGDHLTH,
    USSLOWL_MULTUTIL,
    USSLOWL_OILGSCON,
    USSLOWL_OILGSDRL,
    USSLOWL_OILGSEQP,
    USSLOWL_OILGSEXP,
    USSLOWL_PAPER,
    USSLOWL_PHARMA,
    USSLOWL_PRECMTLS,
    USSLOWL_PSNLPROD,
    USSLOWL_REALEST,
    USSLOWL_RESTAUR,
    USSLOWL_RESVOL,
    USSLOWL_ROADRAIL,
    USSLOWL_SEMICOND,
    USSLOWL_SEMIEQP,
    USSLOWL_SOFTWARE,
    USSLOWL_SPLTYRET,
    USSLOWL_SPTYCHEM,
    USSLOWL_SPTYSTOR,
    USSLOWL_TELECOM,
    USSLOWL_TRADECO,
    USSLOWL_TRANSPRT,
    USSLOWL_WIRELESS
)
SELECT 
    date,
    barrid,
    USSLOWL_BETA,
    USSLOWL_COUNTRY,
    USSLOWL_DIVYILD,
    USSLOWL_EARNQLTY,
    USSLOWL_EARNYILD,
    USSLOWL_GROWTH,
    USSLOWL_LEVERAGE,
    USSLOWL_LIQUIDTY,
    USSLOWL_LTREVRSL,
    USSLOWL_MGMTQLTY,
    USSLOWL_MIDCAP,
    USSLOWL_MOMENTUM,
    USSLOWL_PROFIT,
    USSLOWL_PROSPECT,
    USSLOWL_SIZE,
    USSLOWL_VALUE,
    USSLOWL_AERODEF,
    USSLOWL_AIRLINES,
    USSLOWL_ALUMSTEL,
    USSLOWL_APPAREL,
    USSLOWL_AUTO,
    USSLOWL_BANKS,
    USSLOWL_BEVTOB,
    USSLOWL_BIOLIFE,
    USSLOWL_BLDGPROD,
    USSLOWL_CHEM,
    USSLOWL_CNSTENG,
    USSLOWL_CNSTMACH,
    USSLOWL_CNSTMATL,
    USSLOWL_COMMEQP,
    USSLOWL_COMPELEC,
    USSLOWL_COMSVCS,
    USSLOWL_CONGLOM,
    USSLOWL_CONTAINR,
    USSLOWL_DISTRIB,
    USSLOWL_DIVFIN,
    USSLOWL_ELECEQP,
    USSLOWL_ELECUTIL,
    USSLOWL_FOODPROD,
    USSLOWL_FOODRET,
    USSLOWL_GASUTIL,
    USSLOWL_HLTHEQP,
    USSLOWL_HLTHSVCS,
    USSLOWL_HOMEBLDG,
    USSLOWL_HOUSEDUR,
    USSLOWL_INDMACH,
    USSLOWL_INSURNCE,
    USSLOWL_INTERNET,
    USSLOWL_LEISPROD,
    USSLOWL_LEISSVCS,
    USSLOWL_LIFEINS,
    USSLOWL_MEDIA,
    USSLOWL_MGDHLTH,
    USSLOWL_MULTUTIL,
    USSLOWL_OILGSCON,
    USSLOWL_OILGSDRL,
    USSLOWL_OILGSEQP,
    USSLOWL_OILGSEXP,
    USSLOWL_PAPER,
    USSLOWL_PHARMA,
    USSLOWL_PRECMTLS,
    USSLOWL_PSNLPROD,
    USSLOWL_REALEST,
    USSLOWL_RESTAUR,
    USSLOWL_RESVOL,
    USSLOWL_ROADRAIL,
    USSLOWL_SEMICOND,
    USSLOWL_SEMIEQP,
    USSLOWL_SOFTWARE,
    USSLOWL_SPLTYRET,
    USSLOWL_SPTYCHEM,
    USSLOWL_SPTYSTOR,
    USSLOWL_TELECOM,
    USSLOWL_TRADECO,
    USSLOWL_TRANSPRT,
    USSLOWL_WIRELESS
FROM {{ source_table }} AS s
ON CONFLICT (date, barrid) DO UPDATE SET 
    USSLOWL_BETA = excluded.USSLOWL_BETA,
    USSLOWL_COUNTRY = excluded.USSLOWL_COUNTRY,
    USSLOWL_DIVYILD = excluded.USSLOWL_DIVYILD,
    USSLOWL_EARNQLTY = excluded.USSLOWL_EARNQLTY,
    USSLOWL_EARNYILD = excluded.USSLOWL_EARNYILD,
    USSLOWL_GROWTH = excluded.USSLOWL_GROWTH,
    USSLOWL_LEVERAGE = excluded.USSLOWL_LEVERAGE,
    USSLOWL_LIQUIDTY = excluded.USSLOWL_LIQUIDTY,
    USSLOWL_LTREVRSL = excluded.USSLOWL_LTREVRSL,
    USSLOWL_MGMTQLTY = excluded.USSLOWL_MGMTQLTY,
    USSLOWL_MIDCAP = excluded.USSLOWL_MIDCAP,
    USSLOWL_MOMENTUM = excluded.USSLOWL_MOMENTUM,
    USSLOWL_PROFIT = excluded.USSLOWL_PROFIT,
    USSLOWL_PROSPECT = excluded.USSLOWL_PROSPECT,
    USSLOWL_SIZE = excluded.USSLOWL_SIZE,
    USSLOWL_VALUE = excluded.USSLOWL_VALUE,
    USSLOWL_AERODEF = excluded.USSLOWL_AERODEF,
    USSLOWL_AIRLINES = excluded.USSLOWL_AIRLINES,
    USSLOWL_ALUMSTEL = excluded.USSLOWL_ALUMSTEL,
    USSLOWL_APPAREL = excluded.USSLOWL_APPAREL,
    USSLOWL_AUTO = excluded.USSLOWL_AUTO,
    USSLOWL_BANKS = excluded.USSLOWL_BANKS,
    USSLOWL_BEVTOB = excluded.USSLOWL_BEVTOB,
    USSLOWL_BIOLIFE = excluded.USSLOWL_BIOLIFE,
    USSLOWL_BLDGPROD = excluded.USSLOWL_BLDGPROD,
    USSLOWL_CHEM = excluded.USSLOWL_CHEM,
    USSLOWL_CNSTENG = excluded.USSLOWL_CNSTENG,
    USSLOWL_CNSTMACH = excluded.USSLOWL_CNSTMACH,
    USSLOWL_CNSTMATL = excluded.USSLOWL_CNSTMATL,
    USSLOWL_COMMEQP = excluded.USSLOWL_COMMEQP,
    USSLOWL_COMPELEC = excluded.USSLOWL_COMPELEC,
    USSLOWL_COMSVCS = excluded.USSLOWL_COMSVCS,
    USSLOWL_CONGLOM = excluded.USSLOWL_CONGLOM,
    USSLOWL_CONTAINR = excluded.USSLOWL_CONTAINR,
    USSLOWL_DISTRIB = excluded.USSLOWL_DISTRIB,
    USSLOWL_DIVFIN = excluded.USSLOWL_DIVFIN,
    USSLOWL_ELECEQP = excluded.USSLOWL_ELECEQP,
    USSLOWL_ELECUTIL = excluded.USSLOWL_ELECUTIL,
    USSLOWL_FOODPROD = excluded.USSLOWL_FOODPROD,
    USSLOWL_FOODRET = excluded.USSLOWL_FOODRET,
    USSLOWL_GASUTIL = excluded.USSLOWL_GASUTIL,
    USSLOWL_HLTHEQP = excluded.USSLOWL_HLTHEQP,
    USSLOWL_HLTHSVCS = excluded.USSLOWL_HLTHSVCS,
    USSLOWL_HOMEBLDG = excluded.USSLOWL_HOMEBLDG,
    USSLOWL_HOUSEDUR = excluded.USSLOWL_HOUSEDUR,
    USSLOWL_INDMACH = excluded.USSLOWL_INDMACH,
    USSLOWL_INSURNCE = excluded.USSLOWL_INSURNCE,
    USSLOWL_INTERNET = excluded.USSLOWL_INTERNET,
    USSLOWL_LEISPROD = excluded.USSLOWL_LEISPROD,
    USSLOWL_LEISSVCS = excluded.USSLOWL_LEISSVCS,
    USSLOWL_LIFEINS = excluded.USSLOWL_LIFEINS,
    USSLOWL_MEDIA = excluded.USSLOWL_MEDIA,
    USSLOWL_MGDHLTH = excluded.USSLOWL_MGDHLTH,
    USSLOWL_MULTUTIL = excluded.USSLOWL_MULTUTIL,
    USSLOWL_OILGSCON = excluded.USSLOWL_OILGSCON,
    USSLOWL_OILGSDRL = excluded.USSLOWL_OILGSDRL,
    USSLOWL_OILGSEQP = excluded.USSLOWL_OILGSEQP,
    USSLOWL_OILGSEXP = excluded.USSLOWL_OILGSEXP,
    USSLOWL_PAPER = excluded.USSLOWL_PAPER,
    USSLOWL_PHARMA = excluded.USSLOWL_PHARMA,
    USSLOWL_PRECMTLS = excluded.USSLOWL_PRECMTLS,
    USSLOWL_PSNLPROD = excluded.USSLOWL_PSNLPROD,
    USSLOWL_REALEST = excluded.USSLOWL_REALEST,
    USSLOWL_RESTAUR = excluded.USSLOWL_RESTAUR,
    USSLOWL_RESVOL = excluded.USSLOWL_RESVOL,
    USSLOWL_ROADRAIL = excluded.USSLOWL_ROADRAIL,
    USSLOWL_SEMICOND = excluded.USSLOWL_SEMICOND,
    USSLOWL_SEMIEQP = excluded.USSLOWL_SEMIEQP,
    USSLOWL_SOFTWARE = excluded.USSLOWL_SOFTWARE,
    USSLOWL_SPLTYRET = excluded.USSLOWL_SPLTYRET,
    USSLOWL_SPTYCHEM = excluded.USSLOWL_SPTYCHEM,
    USSLOWL_SPTYSTOR = excluded.USSLOWL_SPTYSTOR,
    USSLOWL_TELECOM = excluded.USSLOWL_TELECOM,
    USSLOWL_TRADECO = excluded.USSLOWL_TRADECO,
    USSLOWL_TRANSPRT = excluded.USSLOWL_TRANSPRT,
    USSLOWL_WIRELESS = excluded.USSLOWL_WIRELESS
;
