register $PIGGYBANK;

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

code_provider_charge = FOREACH no_header GENERATE
    REGEX_EXTRACT (code, '([0-9]+?) .+', 1) AS operationCode,
    providerId AS provider,
    averageCharges AS charges;
            
group_by_code = GROUP code_provider_charge BY operationCode;

top_provider_by_charges = FOREACH group_by_code { 
    orderedTmp = ORDER code_provider_charge BY charges DESC;
    filteredTmp = LIMIT orderedTmp 1;
    GENERATE FLATTEN(filteredTmp);
};

group_by_provider = GROUP top_provider_by_charges BY provider;

providers_with_counts = FOREACH group_by_provider GENERATE
                            group AS provider,
                            COUNT(top_provider_by_charges.operationCode) AS instance_count;

ordered_providers_with_counts = ORDER providers_with_counts BY instance_count DESC;

result = LIMIT ordered_providers_with_counts 3;

store result into 'answers/one_b';