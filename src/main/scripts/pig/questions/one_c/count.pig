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

code_region_provider = FOREACH no_header GENERATE
    REGEX_EXTRACT (code, '([0-9]+?) .+', 1) AS operationCode,
    providerId AS provider,
    hospitalRegion AS region,
    discharges AS discharges,
    averageCharges AS charges;
            
group_by_region_code = GROUP code_region_provider BY (operationCode, region);

region_code_with_total_discharges = FOREACH group_by_region_code {
    totalDischarges = SUM(code_region_provider.discharges);
    GENERATE
        FLATTEN(code_region_provider),
        totalDischarges AS totalDischarges;
};

group_again_by_region_code = GROUP region_code_with_total_discharges BY (operationCode, region);

average_by_region_code = FOREACH group_again_by_region_code {
    weightedCharge = FOREACH region_code_with_total_discharges GENERATE 
                discharges * charges / totalDischarges;
    GENERATE
        FLATTEN(group),
        SUM(weightedCharge) AS avgCharge;
};

group_by_code = GROUP average_by_region_code BY operationCode;

top_region_by_charges = FOREACH group_by_code { 
    orderedTmp = ORDER average_by_region_code BY avgCharge DESC;
    filteredTmp = LIMIT orderedTmp 1;
    GENERATE FLATTEN(filteredTmp);
};

group_by_region = GROUP top_region_by_charges BY region;

region_with_counts = FOREACH group_by_region GENERATE
                            group AS region,
                            COUNT(top_region_by_charges.operationCode) AS instance_count;

ordered_regions_with_counts = ORDER region_with_counts BY instance_count DESC;

result = LIMIT ordered_regions_with_counts 3;

store result into 'answers/one_c';