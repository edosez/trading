import sys, os
from utils.utils import MyLogger, load_config
from pathlib import Path
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import func
from utils.utils import nan_to_none
# import keyring


class DBUtils:

    def __init__(self, table_schema, pandas_df, save_log = True):
        '''
        Constructor method
        '''
        # NAMESPACE = "mariadb"
        # ENTRY = "admin"
        self.table_schema = table_schema
        # cred = keyring.get_credential(NAMESPACE, ENTRY)
        cred = load_config("cred")
        mariadb_user = cred.get("mariaDB").get("user")
        mariadb_pwd = cred.get("mariaDB").get("password")

        self.engine = create_engine("mariadb+pymysql://{0}:{1}@localhost/{2}?charset=utf8mb4".format(mariadb_user, mariadb_pwd, table_schema))

        self.pandas_df = pandas_df

        self.save_log = save_log
        # Defining the absolute path where the pandas profiling configuration files reside
        # the working directory should be offline_modelling\example\models\yyyy_mm_dd_hh_mm_ss and so it needs 3 jumps on top to get to offline_modelling\
        if os.path.basename(os.getcwd()) == 'trading':
            self.parent_path = os.getcwd()
        else:
            self.parent_path = str(Path().resolve().parents[2])

        if save_log:
            # Initiate the logging
            self._logging = MyLogger(log_file='logs/db_utils.log', name='db_utils')
        elif not save_log:
            self._logging = MyLogger(log_file=None, name='db_utils')
        else:
            sys.exit('save_log parameter has not been set correctly | Adjust accordingly to either True or False')
        

    def MetaDataObject(self, table_name):
        '''
        Function that creates a Table object so that it can be referenced when running SQL comandas and make sure the MariaDB keeps consistent.
        
        :param table_name: table name stored on DB that you want to investigate
        :type: str
        :return: TablesOptions, contains the table Metadata an Columns
        :rtype: sqlalchemy Table object   
        '''

        metadata_obj = MetaData()
        metadata_obj.reflect(self.engine, schema=self.table_schema)
        Base = automap_base(metadata=metadata_obj)
        Base.prepare()

        alchemyClassDict = {}
        for t in Base.classes.keys():
            alchemyClassDict[t] = Base.classes[t]

        TableOptions = alchemyClassDict[table_name].__table__

        self._logging.info('Table object created: {0}'.format(TableOptions))

        return TableOptions


    def LoadTable(self, table_name, pk, data_types = None):
        '''
        Create a function that connects to mariaDB given keyring credentials and then load the pandas DataFrame into the DB as a SQL table.

        :param table_name: table name stored on DB that you want to investigate
        :type: str
        :param pk: primary key of the table
        :type: str
        :param data_types: data types of the table in case you want to force some of this into SQL injection. It needs to rely on sqlalchemy objects (e.g. {'pk': String(50)})
        :type: dict
        :return: None
        '''
        self._logging.info("Loading pandas DataFrame into MariaDB in schema {0}".format(self.table_schema))

        inspector = inspect(self.engine)

        if inspector.has_table(table_name):
            self._logging.info("Table {0} already exists in schema {1}".format(table_name, self.table_schema))
        else:
            self.pandas_df.to_sql(table_name, con=self.engine, if_exists='replace', schema=self.table_schema, index=False, dtype=data_types)
            self._logging.info("Table {0} loaded into MariaDB in schema {1}".format(table_name, self.table_schema))
            with self.engine.connect() as con:
                self._logging.info("Adding Primary Key {0} to table {1}".format(pk, table_name))
                con.execute('ALTER TABLE `{0}` ADD PRIMARY KEY ({1});'.format(table_name, ', '.join(x for x in pk)))
                self._logging.info("Primary Key {0} added to table {1}".format(pk, table_name))       
        

    def UpdateInsertTable(self, table_name):
        '''
        Function that performs Upserts on the table. It will update based on pk in the table
        :param table_name: table name stored on DB that you want to investigate
        :type: str
        '''

        table_to_update = self.MetaDataObject(table_name)

        imported_df = nan_to_none(self.pandas_df)

        dict_to_insert = imported_df.to_dict(orient='records')

        insert_stmt = insert(table_to_update).values(dict_to_insert)

        self._logging.info("Insert statement: \n {0} \n".format(insert_stmt.inserted))

        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)

        self._logging.info("On duplicate statement: \n {0} \n".format(on_duplicate_key_stmt))
        
        with self.engine.connect() as con:
            self._logging.info("Executing upsert statement into {0}.{1}".format(self.table_schema, table_name))
            con.execute(on_duplicate_key_stmt)
            self._logging.info("Upsert statement executed")       
    



