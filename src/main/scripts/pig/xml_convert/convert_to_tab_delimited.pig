register $PIGGYBANK;

SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

rows = LOAD '$INPUT'
        USING org.apache.pig.piggybank.storage.XMLLoader('rows') AS (row_xml: chararray);

parsed_rows = FOREACH rows GENERATE
    REGEX_EXTRACT(row_xml, 
        '\\<field.+?\\"id\\"\\>\\s*?(\\d+?)\\s*?\\<\\/field\\>', 1) 
        AS id: chararray,
    REGEX_EXTRACT(row_xml, 
        '\\<field.+?\\"age\\"\\>\\s*?([\\&lt\\;\\-\\+\\d]+?)\\s*?\\<\\/field\\>', 1) 
        AS age: chararray,
    REGEX_EXTRACT(row_xml, 
        '\\<field.+?\\"gndr\\"\\>\\s*?(\\w)\\s*?\\<\\/field\\>', 1) 
        AS gender: chararray,
    REGEX_EXTRACT(row_xml, 
        '\\<field.+?\\"inc\\"\\>\\s*?([\\&lt\\;\\-\\+\\d]+?)\\s*?\\<\\/field\\>', 1) 
        AS income: chararray;
    
STORE parsed_rows INTO '$OUTPUT' USING PigStorage('\\u001');