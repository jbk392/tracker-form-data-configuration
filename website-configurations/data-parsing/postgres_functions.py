import psycopg2
from psycopg2 import errors
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


def check_for_pk(website_pk, cursor, pk_check_query):
    cursor.execute(pk_check_query, (website_pk,))
    return cursor.fetchone()[0] > 0

def insert_into_db(query, data, cursor, cnx):
    try:
        cursor.execute(query, data)
        cnx.commit()
    except Exception as e:
        print("error inserting: ", e)


def shut_down_db(cur, conn):
    cur.close()
    conn.close()

def get_domain_id(cur, conn, domain):
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM domains
            WHERE domain = %s
        );
    """
    cur.execute(query, (domain,))
    exists = cur.fetchone()[0]

    if exists:
        select_query = """
            SELECT id
            FROM domains
            WHERE domain = %s;
        """
        cur.execute(select_query, (domain,))
        result = cur.fetchone()
        return result[0]
    else:
        insert_query = """
            INSERT INTO domains (domain)
            VALUES (%s)
            RETURNING id;
        """
        cur.execute(insert_query, (domain,))
        new_id = cur.fetchone()[0]
        conn.commit()
        return new_id

def setup_postgres(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS domains (
            id SERIAL,
            domain VARCHAR(255)
        )
    """)

    # Meta PII data extracted from JavaScript configuration files
    cursor.execute("""
       CREATE TABLE IF NOT EXISTS meta_static_keys (
          id SERIAL,
          pixel_id TeXT,
          website TEXT,
          date DATE,
          vm_name TEXT,
          key_ TEXT
       )
    """)

    # Meta PII data extracted from network events
    cursor.execute("""
       CREATE TABLE IF NOT EXISTS meta_match_keys (
          id SERIAL,
          pixel_id TeXT,
          website TEXT,
          date DATE,
          vm_name TEXT,
          match_key TEXT,
          mode TEXT
       )
    """)

    # TABLE: website_visits
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS website_visits (
            pk VARCHAR(255) PRIMARY KEY,
            website TEXT,
            date DATE,
            vm_name VARCHAR(255),
            meta_static_collection_status BOOLEAN,
            meta_static_ids TEXT,
            meta_dynamic_collection_status BOOLEAN,
            meta_dynamic_ids TEXT,
            google_static_collection_status BOOLEAN,
            google_static_ids TEXT,
            gtm_static_collection_status BOOLEAN,
            gtm_static_ids TEXT,
            google_dynamic_collection_status BOOLEAN,
            google_dynamic_ids TEXT,
            gtm_present BOOLEAN,
            website_classification VARCHAR(255),
            injected_form_present BOOLEAN,
            file_count INT,
            html_status TEXT,
            visit_status TEXT,
            domains_list TEXT
        )
    """)

    # TABLE: errors
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS errors (
            id SERIAL,
            website TEXT,
            date DATE,
            vm_name VARCHAR(255),
            file_name TEXT,
            error_type VARCHAR(255),
            error VARCHAR(255)
        )
    """)

