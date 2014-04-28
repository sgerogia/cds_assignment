register $PIGGYBANK;

raw_lines = LOAD '$INPUT' 
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

no_header_lines = FILTER raw_lines BY (code MATCHES '[0-9]+? .+');

only_code_lines = FOREACH no_header_lines GENERATE
    REGEX_EXTRACT (code, '([0-9]+?) .+', 1) AS code,
    discharges;
            
group_lines = GROUP only_code_lines BY code;

sum_lines = FOREACH group_lines GENERATE group, COUNT(only_code_lines);

ordered_sum_lines = ORDER sum_lines BY group ASC;

store ordered_sum_lines into 'inpatient_outpatient_charge';