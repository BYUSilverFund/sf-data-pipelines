CREATE TABLE IF NOT EXISTS factors_wide(
    -- IDS
    date DATE,
    -- Risk Factors
    USSLOWL_BETA DOUBLE,
    USSLOWL_COUNTRY DOUBLE,
    USSLOWL_DIVYILD DOUBLE,
    USSLOWL_EARNQLTY DOUBLE,
    USSLOWL_EARNYILD DOUBLE,
    USSLOWL_GROWTH DOUBLE,
    USSLOWL_LEVERAGE DOUBLE,
    USSLOWL_LIQUIDTY DOUBLE,
    USSLOWL_LTREVRSL DOUBLE,
    USSLOWL_MGMTQLTY DOUBLE,
    USSLOWL_MIDCAP DOUBLE,
    USSLOWL_MOMENTUM DOUBLE,
    USSLOWL_PROFIT DOUBLE,
    USSLOWL_PROSPECT DOUBLE,
    USSLOWL_SIZE DOUBLE,
    USSLOWL_VALUE DOUBLE,
    -- Industry Factors
    USSLOWL_AERODEF DOUBLE,
    USSLOWL_AIRLINES DOUBLE,
    USSLOWL_ALUMSTEL DOUBLE,
    USSLOWL_APPAREL DOUBLE,
    USSLOWL_AUTO DOUBLE,
    USSLOWL_BANKS DOUBLE,
    USSLOWL_BEVTOB DOUBLE,
    USSLOWL_BIOLIFE DOUBLE,
    USSLOWL_BLDGPROD DOUBLE,
    USSLOWL_CHEM DOUBLE,
    USSLOWL_CNSTENG DOUBLE,
    USSLOWL_CNSTMACH DOUBLE,
    USSLOWL_CNSTMATL DOUBLE,
    USSLOWL_COMMEQP DOUBLE,
    USSLOWL_COMPELEC DOUBLE,
    USSLOWL_COMSVCS DOUBLE,
    USSLOWL_CONGLOM DOUBLE,
    USSLOWL_CONTAINR DOUBLE,
    USSLOWL_DISTRIB DOUBLE,
    USSLOWL_DIVFIN DOUBLE,
    USSLOWL_ELECEQP DOUBLE,
    USSLOWL_ELECUTIL DOUBLE,
    USSLOWL_FOODPROD DOUBLE,
    USSLOWL_FOODRET DOUBLE,
    USSLOWL_GASUTIL DOUBLE,
    USSLOWL_HLTHEQP DOUBLE,
    USSLOWL_HLTHSVCS DOUBLE,
    USSLOWL_HOMEBLDG DOUBLE,
    USSLOWL_HOUSEDUR DOUBLE,
    USSLOWL_INDMACH DOUBLE,
    USSLOWL_INSURNCE DOUBLE,
    USSLOWL_INTERNET DOUBLE,
    USSLOWL_LEISPROD DOUBLE,
    USSLOWL_LEISSVCS DOUBLE,
    USSLOWL_LIFEINS DOUBLE,
    USSLOWL_MEDIA DOUBLE,
    USSLOWL_MGDHLTH DOUBLE,
    USSLOWL_MULTUTIL DOUBLE,
    USSLOWL_OILGSCON DOUBLE,
    USSLOWL_OILGSDRL DOUBLE,
    USSLOWL_OILGSEQP DOUBLE,
    USSLOWL_OILGSEXP DOUBLE,
    USSLOWL_PAPER DOUBLE,
    USSLOWL_PHARMA DOUBLE,
    USSLOWL_PRECMTLS DOUBLE,
    USSLOWL_PSNLPROD DOUBLE,
    USSLOWL_REALEST DOUBLE,
    USSLOWL_RESTAUR DOUBLE,
    USSLOWL_RESVOL DOUBLE,
    USSLOWL_ROADRAIL DOUBLE,
    USSLOWL_SEMICOND DOUBLE,
    USSLOWL_SEMIEQP DOUBLE,
    USSLOWL_SOFTWARE DOUBLE,
    USSLOWL_SPLTYRET DOUBLE,
    USSLOWL_SPTYCHEM DOUBLE,
    USSLOWL_SPTYSTOR DOUBLE,
    USSLOWL_TELECOM DOUBLE,
    USSLOWL_TRADECO DOUBLE,
    USSLOWL_TRANSPRT DOUBLE,
    USSLOWL_WIRELESS DOUBLE,
    PRIMARY KEY(date)
)