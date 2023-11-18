def test_get_columns(my_testclass) -> None:
    assert my_testclass.get_columns() == ("column1","column2","column3","column4"), "Columns defined in TestClass do not match"

def test_get_dtypes(my_testclass) -> None:
    assert my_testclass.get_dtypes() == {"column1":str, "column2":float, "column3":int, "column4":str}, "Datatypes defined in TestClass do not match"

def test_date_format(my_testclass) -> None:
    assert my_testclass.date_format("20210303") == "2021-03-03", "Conversion to date formate is not correct"
    assert my_testclass.date_format("00000000") == "1900-01-01", "8 0s is supose to return '1900-01-01'"
    assert my_testclass.date_format("202103030") == "1900-01-01", "more then 8 digits is supose to return '1900-01-01'"
    assert my_testclass.date_format("2021030") == "1900-01-01", "less then 8 digits is supose to return '1900-01-01'"

def test_get_insert_script(my_testclass) -> None:
    assert my_testclass.get_insert_script().text == "INSERT INTO test VALUES (:column1,:column2,:column3,:column4);", "text of insert values do not match"