import json
import pendulum
from airflow.decorators import dag, task
from config import db, root_path, root_path_input_data, root_path_archive, tables_info,path_sql_scripts,output_data
import logging
logging.basicConfig(level=logging.DEBUG, filename='myapp.log', format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "classification"],
)
def item_classification(debug=False):

    @task()
    def extract_data_from_files_to_stage():
        from stages.etl.connection import Connection
        from stages.etl.extract_data import Extract_data

        conn = Connection(db)._create_connection()
        extract = Extract_data(root_path, conn, root_path_input_data, root_path_archive,tables_info,db,path_sql_scripts)
        result=extract.my_extract_data_to_stg()
        '''table = extract.create_tabel('items')'''
        '''return table'''
        return f"{result}"

    @task()
    def transfer_filesdata_from_stage_to_dwh(stg_result):
        from stages.etl.connection import Connection
        from stages.etl.extract_data import Extract_data

        conn = Connection(db)._create_connection()
        extract = Extract_data(root_path, conn, root_path_input_data, root_path_archive, tables_info, db,
                               path_sql_scripts)
        result = extract.my_extract_filesdata_from_stg_to_dwh()

        '''table = extract.create_tabel('items')'''
        '''return table'''
        return f"{result}"

    @task()
    def extract_web_data(categories_and_webfilters):
        from stages.parsing.parcer import Parcer
        from stages.etl.connection import Connection
        conn = Connection(db)._create_connection()
        par = Parcer(*categories_and_webfilters)
        data = par.take_website_data()
        table_name = 'STG_'+'_'.join(categories_and_webfilters)
        logger.info(table_name)
        data.to_sql(table_name, con=conn, if_exists='replace')
        return table_name

    @task()
    def create_dwh_parcer_data(list_table):
        from stages.sql_scripts.union_tables import union_tabels
        from stages.etl.connection import Connection
        conn = Connection(db)
        logger.info(list_table)
        tables = union_tabels(list_table)
        s = f"""
        Create table if not EXISTS DWH_ProductsStat as {tables}
        """
        logger.info(s)
        conn.read_sql_script(s, file_script=False)
        return 'DWH_ProductsStat'

    @task()
    def create_outputfile_for_dashboards(dwh_table_for_output_dashboard):
        from stages.sql_scripts.union_tables import union_tabels
        from stages.etl.connection import Connection
        conn = Connection(db)
        conn.save_select_result_to_csv("DWH_ProductsStat",output_data,"ProductsStat.csv")
        return 'ProductsStat.csv'


    stg_file_result = extract_data_from_files_to_stage()
    dwh_file_result = transfer_filesdata_from_stage_to_dwh(stg_file_result)
    item_category = ['shlemy', 'flyagi', 'ryukzaki_i_sumki']
    item_filter = ['new', 'discount', 'popular']
    list3 = [[i, str(j)] for i in item_category for j in item_filter]
    website_tables= extract_web_data.expand(categories_and_webfilters=list3)
    dwh_table_website = create_dwh_parcer_data(website_tables)
    output_dashboard_file=create_outputfile_for_dashboards(dwh_table_website)


prod = item_classification(debug=False)
