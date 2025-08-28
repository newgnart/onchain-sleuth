CREATE OR REPLACE FUNCTION hex_to_numeric(hex_string TEXT)
RETURNS NUMERIC AS $$
DECLARE
    result NUMERIC := 0;
    digit CHAR(1);
    power NUMERIC := 1;
    i INTEGER;
BEGIN
    -- Remove '0x' prefix if present
    hex_string := LOWER(REPLACE(hex_string, '0x', ''));
    
    -- Process from right to left (least significant digit first)
    FOR i IN REVERSE LENGTH(hex_string)..1 LOOP
        digit := SUBSTRING(hex_string FROM i FOR 1);
        
        CASE digit
            WHEN '0' THEN result := result + 0 * power;
            WHEN '1' THEN result := result + 1 * power;
            WHEN '2' THEN result := result + 2 * power;
            WHEN '3' THEN result := result + 3 * power;
            WHEN '4' THEN result := result + 4 * power;
            WHEN '5' THEN result := result + 5 * power;
            WHEN '6' THEN result := result + 6 * power;
            WHEN '7' THEN result := result + 7 * power;
            WHEN '8' THEN result := result + 8 * power;
            WHEN '9' THEN result := result + 9 * power;
            WHEN 'a' THEN result := result + 10 * power;
            WHEN 'b' THEN result := result + 11 * power;
            WHEN 'c' THEN result := result + 12 * power;
            WHEN 'd' THEN result := result + 13 * power;
            WHEN 'e' THEN result := result + 14 * power;
            WHEN 'f' THEN result := result + 15 * power;
        END CASE;
        
        power := power * 16;
    END LOOP;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- -- Usage
SELECT hex_to_numeric (
        '0x0000000000000000000000000000000000000000000000000de0b6b3a7640000'
    )