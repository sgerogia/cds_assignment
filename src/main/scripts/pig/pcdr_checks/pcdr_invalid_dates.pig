REGISTER './target/cds_assignment-1.0-SNAPSHOT.jar';

lines = LOAD '$INPUT' 
        USING uk.co.omnispot.data_science.pig.AsciiDelimitedTextLoader()
        AS (char_date: chararray, 
            patient: chararray, 
            operation: chararray, 
            field1: chararray, 
            field2: chararray);

date_lines = FOREACH lines GENERATE char_date, ToDate(char_date, 'yyyyMMdd') as parsed_date;

date_field_lines = FOREACH date_lines 
    GENERATE 
        char_date,
        GetYear(parsed_date) as year,
        GetMonth(parsed_date) as month,
        GetDay(parsed_date) as day;
            
filtered_lines = FILTER date_field_lines BY 
    (year != 2011) OR (year IS NULL)
    OR (month < 1) OR (month > 12) OR (month IS NULL)
    OR (day < 1) OR (day > 31) OR (day IS NULL);


store filtered_lines into 'pcdr_invalid_dates';