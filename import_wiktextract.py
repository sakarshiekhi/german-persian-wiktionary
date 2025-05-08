import json
import MySQLdb
from tqdm import tqdm
from pathlib import Path
import sys
import logging
import argparse
# Removed os import as environment variables are no longer used for credentials -
# Note: Recommending environment variables or a config file is better practice for sharing.

# --- CONFIGURATION ---
# Default JSONL file path - Update this or use the command-line argument.
# It's recommended to use a relative path or let the user specify the full path.
# Example: Assuming the file is in a 'data' subdirectory relative to the script.
DEFAULT_JSONL_FILE_PATH = Path('./data/raw-wiktextract-data.jsonl') # Replaced hardcoded C:\ path

TRANSLATION_BATCH_SIZE = 1000
TARGET_LANG_CODES = {"en", "de", "fa"} # Languages we want to process and link

# --- DATABASE CONFIGURATION ---
# WARNING: Hardcoding credentials here is NOT secure for production environments.
# For GitHub, replace these with placeholder values or, preferably,
# use environment variables or a separate configuration file not committed to Git.
DB_HOST = "localhost"
# REPLACE WITH YOUR DATABASE USERNAME OR USE ENVIRONMENT VARIABLES/CONFIG
DB_USER = "YOUR_DB_USER"
# REPLACE WITH YOUR DATABASE PASSWORD OR USE ENVIRONMENT VARIABLES/CONFIG
# CONSIDER USING os.environ.get('DB_PASSWORD') IF USING ENVIRONMENT VARIABLES
DB_PASSWORD = "YOUR_DB_PASSWORD" # Replaced hardcoded password

# --- LOGGING SETUP ---
# Map logging level names to logging module constants
LOGGING_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

def main():
    """
    Main function to parse arguments, connect to the database, process the
    Wiktextract JSONL file, and import translations.
    """
    # --- ARGUMENT PARSING ---
    parser = argparse.ArgumentParser(description="Import Wiktextract JSONL data into a MySQL dictionary database (DE/FA via EN bridge).")
    parser.add_argument(
        'jsonl_file',
        nargs='?', # Make the file path optional
        default=DEFAULT_JSONL_FILE_PATH,
        type=Path,
        help=f"Path to the Wiktextract .jsonl file (default: {DEFAULT_JSONL_FILE_PATH})"
    )
    parser.add_argument(
        '--log-level',
        choices=LOGGING_LEVELS.keys(),
        default='WARNING',
        help='Set the logging level (default: WARNING)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true', # Store True when the flag is present
        help='Process the file and simulate database operations without actually writing to the database.'
    )
    parser.add_argument(
        '--db-name',
        default='dictdb',
        type=str,
        help='Name of the MySQL database (default: dictdb)'
    )
    # Optional arguments for database credentials (alternative to hardcoding/env vars)
    parser.add_argument(
        '--db-user',
        type=str,
        help='Database username (overrides script default)'
    )
    parser.add_argument(
        '--db-password',
        type=str,
        help='Database password (overrides script default)'
    )
    parser.add_argument(
        '--db-host',
        type=str,
        help='Database host (overrides script default)'
    )


    args = parser.parse_args()

    # Use command-line args for DB credentials if provided, otherwise use script defaults (placeholders)
    db_user = args.db_user if args.db_user else DB_USER
    db_password = args.db_password if args.db_password else DB_PASSWORD
    db_host = args.db_host if args.db_host else DB_HOST


    # --- INITIALIZATION ---
    # Configure logging based on the command-line argument
    logging.basicConfig(filename='dictionary_import.log', level=LOGGING_LEVELS[args.log_level],
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Open error log file - ensure it's closed in finally
    error_log = None
    try:
        error_log = open('import_error_summary.txt', 'w', encoding='utf-8')
    except IOError as e:
        logging.error(f"Failed to open error log file 'import_error_summary.txt': {e}")
        print(f"ERROR: Failed to open error log file 'import_error_summary.txt': {e}", file=sys.stderr)
        sys.exit(1)

    # --- DATABASE CONNECTION ---
    conn = None
    cursor = None
    if not args.dry_run:
        # Check if placeholder credentials are still present
        if db_user == "YOUR_DB_USER" or db_password == "YOUR_DB_PASSWORD":
            print("ERROR: Database username or password placeholders are still present.")
            print("Please update them in the script, use command-line arguments (--db-user, --db-password), or use environment variables.", file=sys.stderr)
            if error_log:
                error_log.close()
            sys.exit(1)

        print(f"Attempting to connect to database '{args.db_name}' as user '{db_user}'...")
        try:
            conn = MySQLdb.connect(
                host=db_host,
                user=db_user,
                passwd=db_password,
                db=args.db_name,
                charset='utf8mb4',
                use_unicode=True,
                autocommit=False
            )
            cursor = conn.cursor()
            print(f"Successfully connected to database '{args.db_name}'.")
        except MySQLdb.Error as e:
            logging.error(f"DB Connection Failed: {e.args[0]}: {e.args[1]}")
            print(f"ERROR: DB Connection Failed: {e.args[0]}: {e.args[1]}", file=sys.stderr)
            print("Please check database connection details, ensure MySQL server is running,", file=sys.stderr)
            print(f"and that the user '{db_user}' has correct password and permissions on '{args.db_name}'.", file=sys.stderr)
            if error_log:
                 error_log.close()
            sys.exit(1)
        except Exception as e:
            logging.error(f"An unexpected error occurred during database connection: {e}")
            print(f"ERROR: An unexpected error occurred during database connection: {e}", file=sys.stderr)
            if error_log:
                 error_log.close()
            sys.exit(1)


    # --- PROCESSING SETUP ---
    word_cache = {}
    translation_batch = []
    stats = {'processed_lines': 0, 'errors': 0, 'english_entries': 0, 'valid_de_fa_pairs_in_en_entry': 0, 'translation_pairs_batched': 0}

    try:
        # --- FILE PROCESSING LOOP ---
        print(f"Processing file: {args.jsonl_file}")
        # Check if the file exists before opening
        if not args.jsonl_file.is_file():
            logging.error(f"Input file not found at {args.jsonl_file}")
            print(f"ERROR: Input file not found at {args.jsonl_file}", file=sys.stderr)
            stats['errors'] += 1
            # Jump to finally block for cleanup
            raise FileNotFoundError # Re-raise to trigger the main FileNotFoundError except block


        with open(args.jsonl_file, 'r', encoding='utf-8') as f:
            for line in tqdm(f, desc="Processing JSONL"):
                stats['processed_lines'] += 1
                try:
                    data = json.loads(line)

                    # Only process English entries as the source for bridging
                    if data.get("lang_code") != "en":
                        continue

                    english_word = data.get("word")
                    if not english_word:
                        logging.warning(f"Skipping line {stats['processed_lines']}: English entry with no 'word' field.")
                        stats['errors'] += 1
                        error_log.write(f"L{stats['processed_lines']}_WARN_NO_WORD: English entry with no 'word' field.\nLine: {line[:200]}\n")
                        continue

                    stats['english_entries'] += 1

                    # Get English word ID
                    en_id = get_word_id(word_cache, english_word, "en", args.dry_run, cursor)
                    if en_id is None:
                        logging.error(f"Failed to get/create word ID for English word '{english_word[:50]}...' on line {stats['processed_lines']}")
                        stats['errors'] += 1
                        error_log.write(f"L{stats['processed_lines']}_ERROR_GET_EN_ID: Failed to get/create ID for '{english_word}'.\nLine: {line[:200]}\n")
                        continue

                    # Extract translations for German and Persian from the English entry
                    translations = data.get("translations", [])
                    german_translations = [t for t in translations if t.get("code") == "de"]
                    persian_translations = [t for t in translations if t.get("code") == "fa"]

                    # We are looking for English entries that have *both* German and Persian translations listed
                    if not german_translations or not persian_translations:
                        continue

                    stats['valid_de_fa_pairs_in_en_entry'] += 1

                    # Process German translations found in this English entry
                    for trans_entry in german_translations:
                         target_word_candidate = trans_entry.get("word")
                         target_words_text_list_processed = []
                         if isinstance(target_word_candidate, str):
                             stripped_word = target_word_candidate.strip()
                             if stripped_word:
                                 target_words_text_list_processed = [stripped_word]
                         elif isinstance(target_word_candidate, list):
                             for item in target_word_candidate:
                                 if isinstance(item, str):
                                     stripped_item = item.strip()
                                     if stripped_item:
                                         target_words_text_list_processed.append(stripped_item)
                         elif target_word_candidate is not None:
                             logging.warning(f"Unexpected 'word' data format for German translation in line {stats['processed_lines']} for source word '{english_word[:50]}...'. Data: '{target_word_candidate}'. Skipping translation entry.")
                             error_log.write(f"L{stats['processed_lines']}_WARN_DE_TRANS_FORMAT: Unexpected target 'word' format for German translation of '{english_word}'. Data: '{target_word_candidate}'.\n")


                         # Add DE -> EN translation pairs
                         if target_words_text_list_processed:
                             for de_word in target_words_text_list_processed:
                                 de_id = get_word_id(word_cache, de_word, "de", args.dry_run, cursor)
                                 if de_id is not None:
                                     translation_batch.append((de_id, en_id))


                    # Process Persian translations found in this English entry
                    for trans_entry in persian_translations:
                         target_word_candidate = trans_entry.get("word")
                         target_words_text_list_processed = []
                         if isinstance(target_word_candidate, str):
                             stripped_word = target_word_candidate.strip()
                             if stripped_word:
                                 target_words_text_list_processed = [stripped_word]
                         elif isinstance(target_word_candidate, list):
                             for item in target_word_candidate:
                                 if isinstance(item, str):
                                     stripped_item = item.strip()
                                     if stripped_item:
                                         target_words_text_list_processed.append(stripped_item)
                         elif target_word_candidate is not None:
                             logging.warning(f"Unexpected 'word' data format for Persian translation in line {stats['processed_lines']} for source word '{english_word[:50]}...'. Data: '{target_word_candidate}'. Skipping translation entry.")
                             error_log.write(f"L{stats['processed_lines']}_WARN_FA_TRANS_FORMAT: Unexpected target 'word' format for Persian translation of '{english_word}'. Data: '{target_word_candidate}'.\n")

                         # Add EN -> FA translation pairs
                         if target_words_text_list_processed:
                             for fa_word in target_words_text_list_processed:
                                 fa_id = get_word_id(word_cache, fa_word, "fa", args.dry_run, cursor)
                                 if fa_id is not None:
                                     translation_batch.append((en_id, fa_id))


                    # Flush batch if size is reached
                    if len(translation_batch) >= TRANSLATION_BATCH_SIZE:
                        process_batch(conn, translation_batch, args.dry_run, cursor, stats)


                except json.JSONDecodeError:
                    logging.warning(f"Skipping line {stats['processed_lines']} due to JSON parse error.")
                    stats['errors'] += 1
                    error_log.write(f"L{stats['processed_lines']}_WARN_JSON_ERROR: JSON parse error.\nLine: {line[:200]}\n")
                    continue
                except Exception as e:
                    logging.error(f"Unexpected error processing line {stats['processed_lines']}: {e}", exc_info=True)
                    stats['errors'] += 1
                    # Log traceback only if it's not a FileNotFoundError handled elsewhere
                    if not isinstance(e, FileNotFoundError):
                         error_log.write(f"L{stats['processed_lines']}_ERROR_UNEXPECTED: Unexpected error: {e}\nLine: {line[:200]}\nTraceback: {sys.exc_info()[2]}\n")
                    continue

            # Process remaining batch after loop
            if translation_batch:
                process_batch(conn, translation_batch, args.dry_run, cursor, stats)

            # Create direct DE-FA links (only if not dry run)
            if not args.dry_run:
                 if conn and cursor:
                    create_direct_links(conn, cursor)
                 else:
                    logging.warning("Database connection/cursor not available for creating direct links (might be in dry run or connection failed).")


    except FileNotFoundError:
        # File not found error is already logged and counted before the loop
        pass # Handled before the loop, just pass here to proceed to finally
    except Exception as e:
        # Catch any other exceptions that might occur outside the processing loop
        logging.error(f"An unexpected error occurred during script execution: {e}", exc_info=True)
        print(f"ERROR: An unexpected error occurred during script execution: {e}", file=sys.stderr)
        stats['errors'] += 1 # Count unexpected error


    finally:
        # --- CLEANUP ---
        if error_log:
            try:
                error_log.close()
                logging.info("Error log file closed.")
            except Exception as e:
                logging.error(f"Error closing error log file: {e}")

        # Close DB connection and cursor only if they were opened
        if not args.dry_run:
            if cursor:
                try:
                    cursor.close()
                    logging.info("Database cursor closed.")
                except Exception as e:
                    logging.error(f"Error closing cursor: {e}")

            if conn:
                try:
                    conn.close()
                    logging.info("Database connection closed.")
                except Exception as e:
                    logging.error(f"Error closing connection: {e}")

        # --- REPORTING ---
        print(f"\n--- Processing Summary ---")
        print(f"Total lines read from file: {stats['processed_lines']}")
        print(f"English entries processed: {stats['english_entries']}")
        print(f"English entries with both German and Persian translations: {stats['valid_de_fa_pairs_in_en_entry']}")

        if args.dry_run:
            print(f"Translation pairs that would have been batched (DRY RUN): {stats['translation_pairs_batched']}")
        else:
            # Note: The actual number of rows in the DB might be less than
            # translation_pairs_batched due to INSERT IGNORE handling duplicates.
            # To get the exact number, you would need to query the DB after the script finishes.
            print(f"Translation pairs batched for DB (actual inserts may differ due to IGNORE): {stats['translation_pairs_batched']}")


        print(f"Lines/items skipped or with errors: {stats['errors']}")
        print(f"Word cache size: {len(word_cache)}")
        print("--------------------------")
        print("Import process finished.")


# Modified get_word_id to accept cursor
def get_word_id(cache, word, lang, is_dry_run, cursor):
    """
    Retrieves the ID for a word in a specific language from the cache or database.
    If the word does not exist, it is assigned a temporary ID in dry run mode
    or inserted into the database otherwise.
    Returns the word ID or None if an error occurs.
    """
    key = (word.lower(), lang)
    if key in cache:
        return cache[key]

    if is_dry_run:
        # In dry run, assign a unique temporary ID and cache it
        # Use a non-numeric prefix to avoid confusion with real IDs
        fake_id = f"DRYID-{len(cache)+1}"
        cache[key] = fake_id
        return fake_id

    # Use the passed cursor for DB operations
    try:
        cursor.execute(
            "SELECT id FROM words WHERE word_text = %s AND lang_code = %s",
            (word.lower(), lang) # Use parameterized query for safe escaping
        )
        if row := cursor.fetchone():
            word_id = row[0]
            cache[key] = word_id
            return word_id

        # If not found, insert the word
        cursor.execute(
            "INSERT INTO words (word_text, lang_code) VALUES (%s, %s)",
            (word.lower(), lang) # Use parameterized query for safe escaping
        )
        # No explicit commit here, handled by process_batch or main loop flush
        word_id = cursor.lastrowid
        cache[key] = word_id
        return word_id

    except MySQLdb.IntegrityError as e:
        # This happens if the word was inserted by another process/thread
        # or in a race condition. Re-select to get the ID.
        if e.args[0] == 1062: # Check for duplicate entry error code
            try:
                # Re-select to get the ID of the already existing word
                cursor.execute(
                    "SELECT id FROM words WHERE word_text = %s AND lang_code = %s",
                    (word.lower(), lang) # Use parameterized query
                )
                if row := cursor.fetchone():
                    word_id = row[0]
                    cache[key] = word_id
                    return word_id
                else:
                     # This case should ideally not happen after an IntegrityError 1062
                     logging.warning(f"Integrity Error 1062 but word not found after re-select for '{word[:50]}...' ({lang})")
                     return None # Still couldn't find it after retry
            except Exception as re:
                logging.error(f"Error during re-select after IntegrityError for '{word[:50]}...' ({lang}): {re}")
                return None
        else:
             # Log other IntegrityErrors
             logging.error(f"MySQL Integrity Error processing word '{word[:50]}...' ({lang}): {e.args[0]}, {e.args[1]}")
             return None

    except Exception as e:
        logging.error(f"Word insert/select failed for '{word[:50]}...' ({lang}): {e}")
        return None

# Modified process_batch to accept cursor and stats
def process_batch(conn, batch, is_dry_run, cursor, stats):
    """
    Processes a batch of translation pairs (simulates insert in dry run,
    inserts into DB otherwise).
    """
    if not batch:
        return

    try:
        if is_dry_run:
            # In dry run, just count the pairs that would have been inserted
            stats['translation_pairs_batched'] += len(batch)
            # logging.info(f"DRY RUN: Would have flushed batch of {len(batch)} translations.")
        else:
            # Normal database interaction
            try:
                sql = "INSERT IGNORE INTO translations (source_id, target_id) VALUES (%s, %s)"
                # The translation_batch contains word IDs (integers or dry run strings).
                # In dry run, we wouldn't reach this database execution part.
                cursor.executemany(sql, batch)
                conn.commit() # Commit the batch
                stats['translation_pairs_batched'] += len(batch) # Increment actual count batched
                # logging.info(f"Flushed batch of {len(batch)} translations.")
            except Exception as e:
                # Check if conn is available before rollback
                if conn:
                     conn.rollback() # Rollback the batch on error
                logging.error(f"Batch insert failed: {e}")
    finally:
        batch.clear() # Clear the batch regardless of mode or success

# Modified create_direct_links to accept cursor
def create_direct_links(conn, cursor):
    """
    Creates direct DE-FA translation links based on existing DE-EN and EN-FA links.
    """
    print("Creating direct DE-FA links via English bridge...")
    logging.info("Creating direct DE-FA links via English bridge...")
    try:
        # Use the passed cursor for DB operations
        sql = """
            INSERT IGNORE INTO translations (source_id, target_id)
            SELECT de.id, fa.id
            FROM translations de_en
            JOIN words de ON de_en.source_id = de.id
            JOIN words en ON de_en.target_id = en.id
            JOIN translations en_fa ON en_fa.source_id = en.id
            JOIN words fa ON en_fa.target_id = fa.id
            WHERE de.lang_code = 'de' AND en.lang_code = 'en' AND fa.lang_code = 'fa'
        """
        cursor.execute(sql)
        conn.commit()
        created_count = cursor.rowcount
        print(f"Created {created_count} direct DE-FA links.")
        logging.info(f"Created {created_count} direct DE-FA links.")
    except Exception as e:
        # Check if conn is available before rollback
        if conn:
            conn.rollback()
        logging.error(f"Direct links creation failed: {e}")


if __name__ == '__main__':
    main()