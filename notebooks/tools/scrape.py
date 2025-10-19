import json
import requests
import sqlite3

import bs4 as bs
import pandas as pd

# This import is a bit weird because the __main__ entry point will be in
# notebooks/ dir so the imports have to be absolute from that perspective.
from tools.config import SAVE_DIR


def create_db():
    """Creates a sqlite3 database in the SAVE_DIR folder.

    Note: This does not automatically delete existing db files, this
    must be done manually to avoid unintentional deletion of data.
    """
    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS Years (
                year_id INTEGER PRIMARY KEY,
                year    INTEGER UNIQUE
            ) STRICT
        """)
    
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS Candidates (
                candidate_id INTEGER PRIMARY KEY,
                name         TEXT UNIQUE,
                ethnicity    TEXT
            ) STRICT
        """)
    
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS Municipalities (
                municipality_id INTEGER PRIMARY KEY,
                year_id         INTEGER,
                name            TEXT,
                FOREIGN KEY (year_id) REFERENCES Years (year_id),
                UNIQUE(name, year_id)
            ) STRICT
        """)
    
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS CandidateResults (
                result_id       INTEGER PRIMARY KEY,
                municipality_id INTEGER,
                candidate_id    INTEGER,
                votes           INTEGER,
                FOREIGN KEY (municipality_id) REFERENCES Municipalities (municipality_id),
                FOREIGN KEY (candidate_id) REFERENCES Candidates (candidate_id)
                UNIQUE(municipality_id, candidate_id, votes)
            ) STRICT
        """)
    
    conn.commit()
    conn.close()


def insert_candidates(candidates: list[tuple()]):
    """Inserts a list of candidates which adhere to the following format:

        (name, ethnicity)
        Ethnicity should be in the context of the three constituent people,
        Bosniak (B), Croat (C), and Serb (S).
    """
    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    sql_query = """
            INSERT INTO Candidates 
                (name, ethnicity)
            VALUES (?, ?)
            ON CONFLICT(name) DO NOTHING
        """
    
    try:
        cursor.executemany(sql_query, candidates)
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} candidates.")
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()


def insert_year(year: int):
    """Insert a year into the db."""
    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    sql_query = """
            INSERT INTO Years 
                (year)
            VALUES (?)
            ON CONFLICT(year) DO NOTHING
        """
    
    try:
        cursor.execute(sql_query, (year,))
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} year.")
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()


def get_year_id(year: int):
    """Helps when adding municipalities to a new year by getting the year_id for a given year."""
    year_id = None
    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    sql_query = """
            SELECT * FROM Years
            WHERE Years.year = (?)
        """
    
    try:
        cursor.execute(sql_query, (year,))
        year_id = cursor.fetchone()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()

    return year_id[0]


def insert_municipalities(year: int, municipalities):
    """Expects municipalites to be preprocessed as the names on the website
       are not standardized.
    """
    current_year_id = get_year_id(year)
    batch = [(name, current_year_id) for name in municipalities]

    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    sql_query = """
            INSERT INTO Municipalities 
                (name, year_id)
            VALUES (?, ?)
            ON CONFLICT(name, year_id) DO NOTHING
        """
    
    try:
        cursor.executemany(sql_query, batch)
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} municipalites.")
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()


def get_municipality_id_per_year(year: int) -> dict:
    year_id = get_year_id(year)
    municipalites_this_year = None

    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    sql_query = """
            SELECT * FROM Municipalities
            WHERE Municipalities.year_id = (?)
        """
    
    try:
        cursor.execute(sql_query, (year_id,))
        conn.commit()
        municipalites_this_year = cursor.fetchall()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()

    return {name : municip_id for municip_id, _, name in municipalites_this_year}


def get_candidate_ids() -> dict:
    """To make it easier to insert into CandidateResults, candidate ids are needed."""
    candidates_id = None

    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    sql_query = """
            SELECT * FROM Candidates
        """
    
    try:
        cursor.execute(sql_query)
        conn.commit()
        candidates_id = cursor.fetchall()
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()

    return {name : can_id for can_id, name, _ in candidates_id}


def insert_candidate_results(candidate_results, municipality, year):
    candidate_ids = get_candidate_ids()
    municipality_ids = get_municipality_id_per_year(year)    
    batch = [
        (
            municipality_ids[municipality], 
            candidate_ids[name], 
            votes
        ) 
        for name, votes in candidate_results.items()
    ]
    
    conn = sqlite3.connect(SAVE_DIR + 'political_data.db')
    conn.execute('PRAGMA foreign_keys = ON')
    
    cursor = conn.cursor()
    
    sql_query = """
            INSERT INTO CandidateResults 
                (municipality_id, candidate_id, votes)
            VALUES (?, ?, ?)
            ON CONFLICT(municipality_id, candidate_id, votes) DO NOTHING
        """
    
    try:
        cursor.executemany(sql_query, batch)
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} candidate results for {municipality}.")
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    
    finally:
        conn.close()


def modern_scrape(year: int, api_year: str, municipalities_to_codes: dict):
    """Calls the apis for the 2022 or 2018 race to get voting data for each 
    candidate per municipality.

    Args:
        year: needed to get the correct municipalites for the year
        api_key: Needed to differentiate between 2022 and 2018 data for the api
            endpoint.
        municipalities_to_codes: names to go into the db and their
            corresponding codes to be able to query the api endpoint.
    """
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.izbori.ba/Rezultati_izbora/?resId=32&langId=4',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
        'Sec-Fetch-Mode': 'cors'
    }
    
    for municipality, code in municipalities_to_codes.items():
        api_url = f'https://www.izbori.ba/api_2018/race1_electoralunitcandidatesresult/{api_year}/{code}/4'
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
        
            data = response.json()
            candidate_resuslts = {result["name"] : result["totalVotes"] for result in data}
            insert_candidate_results(candidate_resuslts, municipality, year)
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        except requests.exceptions.JSONDecodeError:
            print("Failed to decode JSON. The response was not JSON.")
            print(response.text)