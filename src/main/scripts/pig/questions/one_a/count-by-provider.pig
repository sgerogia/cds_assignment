register $PIGGYBANK;
register './target/jars/datafu-1.2.0.jar';

define VARIANCE datafu.pig.stats.VAR();
define MEDIAN datafu.pig.stats.Median();

raw = LOAD '$INPUT' 
        USING org.apache.pig.piggybank.storage.CSVLoader() 
        AS (code: chararray, 
            providerId: chararray, 
            providerName: chararray, 
            providerStreetAddress: chararray, 
            providerCity: chararray,
            providerState: chararray,
            providerZip: chararray,
            hospitalRegion: chararray,
            discharges: int,
            averageCharges: double,
            averagePayments: double);

no_header = FILTER raw BY (code MATCHES '[0-9]+? .+');

code_and_charge = FOREACH no_header GENERATE
    REGEX_EXTRACT (code, '([0-9]+?) .+', 1) AS code,
    averageCharges AS charges;
            
group_by_code = GROUP code_and_charge BY code;

order_by_charge = FOREACH group_by_code { 
    tmp = ORDER code_and_charge BY charges ASC;
    GENERATE tmp AS data;
};

rel_var = FOREACH order_by_charge {
    median = MEDIAN(data.charges);
    variance = VARIANCE(data.charges);
    code = DISTINCT data.code;
    GENERATE
        FLATTEN (code) AS code,
        variance / median.quantile_0_5 AS rel_variance;
};

order_rel_var = ORDER rel_var BY rel_variance DESC;

result = LIMIT order_rel_var 3;

store result into 'answers/one_a';