import re
from sqlalchemy import create_engine, text, MetaData, Table

class SQLWrite:
    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

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

    def write_to_postgresql(self, name_sensor, df_general_period):

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {name_sensor} (
                Timestamp TIMESTAMP NOT NULL,
                {name_sensor} REAL NOT NULL
            );
        """
        self.conn.execute(text(create_table_query))

        # Заполняем таблицу значениями из df_general_period
        for index, row in df_general_period.iterrows():
            insert_query = f"""
                INSERT INTO {name_sensor} (Timestamp, {name_sensor})
                VALUES ('{row['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')}', {row['Measurements']});
            """
            self.conn.execute(text(insert_query))

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

    def kill_connection(self):
        self.conn.close()
        print(100*'-')
        print('SQL connection killed')
