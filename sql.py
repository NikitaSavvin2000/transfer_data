from lib2to3.pytree import Base
import re

import pandas as pd
from matplotlib import table
from numpy import datetime64, float64
from sqlalchemy import Float, Integer, String, create_engine, engine_from_config, text, MetaData, Table, Column, \
    TIMESTAMP, Boolean, DateTime, Interval, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Table, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Table

class SQLWrite:
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        self.metadata = MetaData()
        self.Base = declarative_base()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()


    def create_connection(self):
        self.conn = self.engine.connect()
        if self.conn is not None:
            print('Connection with SQL established')
        else:
            print('No connection established')

    def commit(self):
        if self.conn is not None:
            trans = self.conn.begin()
            try:
                trans.commit()
                print('Changes committed')
            except:
                trans.rollback()
                raise
        else:
            print('No connection established')

    '''def create_table(self, table_name, names_columns):
        # Connect to the PostgreSQL database
        # Check if the table exists
        metadata = MetaData(bind=self.engine)
            # If the table does not exist, create it
        table = Table(table_name, metadata, Column('id', Integer, primary_key=True))
        if table_name : # нужно проверить колонку на содержание в ее :
        if table_name == "ListSensors":
            for column in names_columns:
                table.append_column(Column(column, String))
            metadata.create_all()
        table.append_column(Column(names_columns[0], TIMESTAMP))
        table.append_column(Column(names_columns[1], Float))
        metadata.create_all()'''


    def table_list(self):
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        table_names = metadata.tables.keys()
        return list(table_names)

    def saving_data(self):
        if self.conn is not None:
            self.commit()
        else:
            print('No connection established')


    def select_last_date(self, table_name):
        table_list = self.table_list()
        if table_name not in table_list:
            last_date = None
        else:
            query = text(f"SELECT timestamp FROM {table_name} ORDER BY timestamp DESC LIMIT 1")
            result = self.engine.connect().execute(query).fetchone()
            if result is not None:
                last_date = result[0]
            else:
                last_date = None
        return last_date

    def create_table(self, table_name):
        if self.exists_table(table_name):
            table = Table(table_name, self.metadata, autoload_with=None)
            table.create(self.engine)
            print(f'Table "{table_name}" created')
        else:
            print(f'Table "{table_name}" already exists')

    def exists_table(self, table_name):
        if self.metadata.tables.get(table_name) is not None:
            return True
        else: 
            return False


    '''def create_column(self, table_name, name_column, type_column):
        if not self.exists_table(table_name):
            self.create_table(table_name)
        table = Table(table_name, self.metadata, autoload=self.metadata)
        type_column = String()
        metadata = self.metadata
        table = Table(table_name, self.metadata, extend_existing=True,         
              Column(name_column, type_column))
        table = Table(
            table_name, 
            self.metadata,
            Column(name_column, type_=type_column, nullable=True),
            extend_existing=True
                    )'''
    def create_column(self, table_name, name_column, type_column):
        Base = declarative_base()
        Session = sessionmaker(bind=self.engine)
        session = Session()
    
        if not self.exists_table(table_name):
            self.create_table(table_name)
    
        type_column = dict_sql_types[type_column]
        print(name_column)
        print(type_column)
    
        new_row = Table(table_name, Base.metadata,
                    Column(name_column, type_column),
                    extend_existing=True)
    
    # Создаем новую колонку в таблице
        Base.metadata.create_all(self.engine)
        self.session.add(new_row)
    # Commit the changes to save the new row to the database
        self.session.commit()

    from sqlalchemy import text

    def add_columns_to_existing_table(self, table_name, df):
        table_check = Table(table_name, self.metadata, autoload_with=self.engine)
        existing_columns = [c.name for c in table_check.columns]
        new_columns = [col_name for col_name in df.columns if col_name not in existing_columns]
        print(table_name)
        print(table_check)
        print(list(table_check.columns))
        print(new_columns)
        if not new_columns:
            print("No new columns to add")
            return
        with self.engine.begin() as conn:
            for col_name in new_columns:
                if col_name not in existing_columns:
                    col_type = df.dtypes[col_name]
                    sql_type = dict_sql_types[str(col_type)]
                    conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {sql_type.__visit_name__}'))
                    print(f"Adding column '{col_name}' to table '{table_name}'")
        conn.commit()

    def add_custom_columns(self, table_name, col_name):
        table_check = Table(table_name, self.metadata, autoload_with=self.engine)
        col_names = [col.name for col in table_check.columns]
        if col_name in col_names:
            print(f"Column '{col_name}' already exists in table '{table_name}'")
            return
        print('Choose column type')
        print('Column types:')
        print('1 - Number, 2 - Text, 3 - Time')
        response = int(input('Enter number - '))
        if response in [1, 2, 3]:
            sql_type = chose_sql_types[response]
            with self.engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {sql_type.__visit_name__}'))
                print(f"Adding column '{col_name}' to table '{table_name}'")
        conn.commit()

    from sqlalchemy import text
    def update_read_flag(self, sensor_ids):
        table = Table('table_of_sensors', self.metadata, autoload_with=self.engine)
        update_stmt = table.update().where(table.c.SensorId.in_(sensor_ids)).values(Read_flag='Y')
        with self.engine.connect() as conn:
            conn.execute(update_stmt)
            conn.commit()
        print(f'Successful : in values {sensor_ids} added Read flag Y')
    def get_dataframe_from_sql_table(self, table_name):
        # check if the database connection is active
        try:
            self.engine.connect()
            print("Successfully: Connected to database.")
        except:
            print("Error: Unable to connect to database.")
            return None

        # read data from the specified table using pd.read_sql()
        query = text(f"SELECT * FROM {table_name}")
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def create_table_with_columns(self, df, table_name):
        columns = []
        for col_name, col_type in zip(df.columns, df.dtypes):
            sql_type = dict_sql_types[str(col_type)]
            column = Column(col_name, sql_type)
            columns.append(column)

        table = Table(table_name, self.Base.metadata, *columns, extend_existing=True)
        self.Base.metadata.create_all(self.engine)
        self.metadata.create_all()
        self.session.commit()
        return table



    def show_col_name(self, table_name):
        table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
        columns = table.columns
        return columns
    
    def write_to_table(self, table_name, df_data):
        columns_name = df_data.columns.tolist()
        if not self.exists_table(table_name):
            self.create_table(table_name)
            for name_column in columns_name:
                type_column = str(df_data[name_column].dtypes)
                self.create_column(table_name, df_data)
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        with self.engine.begin() as conn:
            for index, row in df_data.iterrows():
                row_data = {col: row[col] for col in df_data.columns}
                ins = table.insert().values(**row_data)
                conn.execute(ins)
        conn.commit()
        print(f"{len(df_data)} rows inserted into '{table_name}'")

    def table_list(self):
        with self.engine.connect() as conn:
            conn = self.engine.connect()
            result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            table_names = [row[0] for row in result]
            conn.close()
        return table_names

    def kill_connection(self):
        self.conn.close()
        print(100*'-')
        print('SQL connection killed')

dict_sql_types = {
    'int64': Integer,
    'float64': Float,
    'bool': Boolean,
    'datetime64[ns]': DateTime,
    'timedelta[ns]': Interval,
    'object': Text
}

chose_sql_types = {
    'int64': Integer,
    1: Float,
    'bool': Boolean,
    3 : DateTime,
    'timedelta[ns]': Interval,
    2: Text
}

