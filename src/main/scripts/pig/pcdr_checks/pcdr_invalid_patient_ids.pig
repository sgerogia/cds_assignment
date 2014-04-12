REGISTER './target/cds_assignment-1.0-SNAPSHOT.jar';

lines   = LOAD '$INPUT' 
        USING uk.co.omnispot.data_science.pig.AsciiDelimitedTextLoader()
        AS (date: chararray, 
            patient: chararray, 
            operation: chararray, 
            field1: chararray, 
            field2: chararray);
            
filtered_lines = FILTER lines BY ((long)patient IS NULL);

store filtered_lines into 'pcdr_invalid_patient_ids';
