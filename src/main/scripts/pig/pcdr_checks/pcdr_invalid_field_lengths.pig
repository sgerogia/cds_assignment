REGISTER './target/cds_assignment-1.0-SNAPSHOT.jar';

lines = LOAD '$INPUT' 
        USING uk.co.omnispot.data_science.pig.AsciiDelimitedTextLoader()
        AS (date: chararray, 
            patient: chararray, 
            operation: chararray, 
            field1: chararray, 
            field2: chararray);
            
filtered_lines = FILTER lines BY 
        (SIZE(date) != 8) 
        OR (SIZE(patient) != 9)
        OR (SIZE(operation) != 4);

store filtered_lines into 'pcdr_invalid_field_lengths';
