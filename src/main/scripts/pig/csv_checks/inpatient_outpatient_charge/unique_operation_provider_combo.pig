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
    providerId AS provider,
    discharges;
            
group_lines = GROUP only_code_lines BY (code, provider);

sum_lines = FOREACH group_lines GENERATE group, COUNT(only_code_lines) AS instances;

only_double_entry_lines = FILTER sum_lines BY instances > 1;

store only_double_entry_lines into 'inpatient_outpatient_charge_unique_codes';