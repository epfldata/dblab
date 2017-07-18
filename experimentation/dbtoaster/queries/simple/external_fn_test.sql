CREATE FUNCTION make_me_an_int (str string) RETURNS int AS EXTERNAL 'ATOI';

SELECT make_me_an_int('3');