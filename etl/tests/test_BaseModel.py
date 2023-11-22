from models.BaseModel import *
from sqlalchemy import text
import pytest

class TestClass(BaseModel):
    table_name:str="test"
    schema:dict={"c1":VARCHAR(3), "c2":FLOAT(), "c3":INTEGER(), "c4":DATE()}
    fk:tuple=("c3",)

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

@pytest.mark.dependency(on=["./test_database.py::test_creat_insert_aditional_tables"], scope="session")
def test_get_fk_values(my_testclass, my_engine) -> None:
    assert my_testclass.get_fk_values(my_testclass.fk[0], my_engine) == {1}, "Foreign Key values do not correspond to '{1}'"

@pytest.mark.dependency(depends=["test_get_fk_values"])
@pytest.mark.parametrize("value, expected, failed", ((1,1,"Value 1 is not on Foreign Keys"),(2,0,"Substitute value is supose to be 0")))
def test_check_fk(my_testclass, my_engine, value, expected, failed) -> None:
    fk_values = my_testclass.get_fk_values(my_testclass.fk[0], my_engine)
    assert my_testclass.check_fk(value, 0, fk_values) == expected, failed

@pytest.mark.dependency(on=["./test_database.py::test_create_insert_aditional_tables"], scope="session")
def test_create_table(my_testclass, my_engine) -> None:
    qry_fk = text(f"""SELECT
    kcu.column_name
FROM information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name='{my_testclass.table_name}';""")

    qry_columns = text(f"""SELECT
    column_name
FROM information_schema.columns
WHERE table_name='{my_testclass.table_name}'
ORDER BY column_name;""")

    my_testclass.create_table(my_engine)
    with my_engine.connect() as conn:
        res_fk = conn.execute(qry_fk).fetchall()[0][0]
        res_columns = tuple(c[0] for c in conn.execute(qry_columns).fetchall())
        conn.execute(text(f"DROP TABLE {my_testclass.table_name};"))
        conn.execute(text(f"DROP TABLE id_{my_testclass.fk[0]};"))
    assert res_fk == my_testclass.fk[0], "Foreign Key doesn't match"
    assert res_columns == my_testclass.get_columns(), "columns doesn't match"