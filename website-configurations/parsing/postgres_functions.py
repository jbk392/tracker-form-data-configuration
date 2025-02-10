import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def connect_to_db():
    conn = psycopg2.connect(
        host=os.environ.get("db_host"),
        user=os.environ.get("db_username"),
        password=os.environ.get("db_password"),
        dbname=os.environ.get("db_name"),
        connect_timeout=300,
        sslmode='require'
    )
    conn.autocommit = True
    cur = conn.cursor()

    return cur, conn

def insert_into_db(query, data, cursor, cnx):
    try:
        cursor.execute(query, data)
        cnx.commit()
    except Exception as e:
        print("error inserting: ", e)

def shut_down_db(cur, conn):
    cur.close()
    conn.close()

def setup_postgres(cursor):
    #### CREATE TABLES ####
    ##### 
    # TABLE 1: meta_dynamic_collection_data_tbl
    # Contains details on keys extracted from meta fdc network event
    ######
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta_collection_data (
            id SERIAL,
            website TEXT,
            date DATE,
            vm_name TEXT,
            match_key TEXT,
            mode TEXT,
            match_value BOOLEAN,
            pixel_id TEXT
        )
    """)

    ##### 
    # TABLE 2: meta_static_collection_data_tbl
    # Contains details on keys extracted from meta config file
    ######
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta_collection_data_tbl (
            id SERIAL,
            website TEXT,
            date DATE,
            vm_name TEXT,
            key TEXT,
            pixel_id TEXT
        )
    """)

    ##### 
    # TABLE 3: website_visits_tbl
    # Contains outcome of each individual website visit
    ######
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS website_visits_tbl (
            pk VARCHAR(255) PRIMARY KEY,
            website TEXT,
            date DATE,
            vm_name VARCHAR(255),
            meta_config BOOLEAN,
            google_config BOOLEAN,
            meta_collect BOOLEAN,
            google_collect BOOLEAN,
            html_status TEXT,
            file_count INT,
            error_type TEXT
        )
    """)

    ##### 
    # TABLE 4: errors_tbl
    # Errors encountered while parsing
    ######
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS errors_tbl (
            website TEXT,
            date DATE,
            vm_name TEXT,
            details TEXT
        )
""")
