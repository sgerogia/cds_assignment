REGISTER './target/cds_assignment-1.0-SNAPSHOT.jar';

lines   = LOAD '$INPUT' 
        USING uk.co.omnispot.data_science.pig.AsciiDelimitedTextLoader()
        AS (date: chararray, 
            patient: chararray, 
            operation: chararray, 
            field1: chararray, 
            field2: chararray);
            
filtered_lines = FILTER lines BY 
        NOT (field1 IS NULL)
        OR NOT (field2 IS NULL);

store filtered_lines into 'pcdr_non_empty_final_lengths';
