register $PIGGYBANK;

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
    
incorrect_rows = FILTER parsed_rows BY
    (id IS NULL) OR (id == '')
    OR (age IS NULL) OR (age == '')
    OR (gender IS NULL) OR (gender == '')
    OR (income IS NULL) OR (income == ''); 

STORE incorrect_rows INTO 'four_fields_per_row';