import sqlite3
import csv
import os

CSV_FILE = "rakibolana.csv"
DB_FILE = "rakibolana.db"

def create_table(cursor):
    """
    Creates the 'words' table in the SQLite database.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            letter TEXT NOT NULL,
            page INTEGER NOT NULL,
            index_on_page INTEGER NOT NULL,
            word TEXT NOT NULL,
            definition TEXT NOT NULL
        )
    """)

def insert_data(cursor, csv_reader):
    """
    Reads data from the CSV reader and inserts it into the 'words' table.
    """
    next(csv_reader) 
    
    insert_sql = """
        INSERT INTO words (letter, page, index_on_page, word, definition)
        VALUES (?, ?, ?, ?, ?)
    """
    
    for i, row in enumerate(csv_reader):
        try:
            letter = row[0]
            page = int(row[1])
            index_on_page = int(row[2])
            word = row[3]
            definition = row[4]
            cursor.execute(insert_sql, (letter, page, index_on_page, word, definition))
        except (ValueError, IndexError) as e:
            print(f"  [WARN] Skipping row {i+2} due to error: {e}. Row content: {row}")
        except sqlite3.Error as e:
            print(f"  [ERROR] SQLite error inserting row {i+2}: {e}. Row content: {row}")


def main():
    """
    Main function to orchestrate the CSV to SQLite conversion.
    """
    if not os.path.exists(CSV_FILE):
        print(f"[ERROR] CSV file '{CSV_FILE}' not found. Please ensure it exists in the current directory.")
        return

    print(f"Starting conversion of '{CSV_FILE}' to '{DB_FILE}'...")
    
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        create_table(cursor)
        print("Table 'words' ensured to exist.")
        
        cursor.execute("DELETE FROM words")
        print("Cleared existing data from 'words' table for a fresh import.")

        with open(CSV_FILE, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            print("Reading CSV and inserting data into SQLite...")
            insert_data(cursor, csv_reader)
        
        conn.commit()
        print("Data imported successfully and changes committed.")
        
    except sqlite3.Error as e:
        print(f"[CRITICAL ERROR] SQLite operation failed: {e}")
    except Exception as e:
        print(f"[CRITICAL ERROR] An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
    
    print("Conversion process finished.")

if __name__ == "__main__":
    main()
