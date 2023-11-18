from models.BaseModel import *
import pytest

class TestClass(BaseModel):
    table_name:str="test"
    schema:dict={"c1":VARCHAR(), "c2":FLOAT(), "c3":INTEGER(), "c4":DATE()}
    fk:tuple=("c3")

@pytest.fixture
def my_testclass() -> TestClass:
    return TestClass()

def test_get_columns(my_testclass) -> None:
    assert my_testclass.get_columns() == ("c1","c2","c3","c4"), "Columns defined in TestClass do not match"

def test_get_dtypes(my_testclass) -> None:
    assert my_testclass.get_dtypes() == {"c1":str, "c2":float, "c3":int, "c4":str}, "Datatypes defined in TestClass do not match"

@pytest.mark.parametrize("date_text, expected, failed", (("20210303","2021-03-03","Conversion to date formate is not correct"),
                                                         ("00000000","1900-01-01","8 0s is supose to return '1900-01-01'"),
                                                         ("202103030","1900-01-01","more then 8 digits is supose to return '1900-01-01'"),
                                                         ("2021030","1900-01-01","less then 8 digits is supose to return '1900-01-01'")))
def test_date_format(my_testclass, date_text, expected, failed) -> None:
    assert my_testclass.date_format(date_text) == expected, failed

def test_get_insert_script(my_testclass) -> None:
    assert my_testclass.get_insert_script().text == "INSERT INTO test VALUES (:c1,:c2,:c3,:c4);", "text of insert values do not match"

@pytest.mark.dependency(on=["./test_database.py::test_create_fk_table"], scope="session")
def test_get_fk_values(my_testclass, my_engine) -> None:
    assert my_testclass.get_fk_values(my_testclass.fk, my_engine) == {1}