import json
import sys
import logging
import argparse
import os # Re-import os if you plan to use environment variables again, otherwise remove
from tqdm import tqdm
from pathlib import Path

# Attempt to import MySQLdb, provide helpful error if not installed
try:
    import MySQLdb
except ImportError:
    print("ERROR: The 'mysqlclient' library is not installed. Please install it using 'pip install mysqlclient'.", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"ERROR: An unexpected error occurred while importing 'mysqlclient': {e}", file=sys.stderr)
    sys.exit(1)


# --- CONFIGURATION ---
# Default JSONL file path - can be overridden by command-line argument.
# IMPORTANT: Ensure this path is correct for your system.
DEFAULT_JSONL_FILE_PATH = Path(r'C:\T-I-M-O-C\fadeu\data\raw-wiktextract-data.jsonl')

TRANSLATION_BATCH_SIZE = 1000

# --- DATABASE CONFIGURATION (HARDCODED FOR LOCAL USE - NOT FOR GITHUB/PRODUCTION) ---
# WARNING: Hardcoding credentials is NOT secure for production environments.
# This is reverted for your convenience based on previous conversation state.
DB_HOST = "localhost"
DB_USER = "sakar"
DB_PASSWORD = "A.a123456" # Your specific hardcoded password
DB_NAME = "dictdb" # Added DB name here for clarity

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
        default=DB_NAME, # Use the DB_NAME constant
        type=str,
        help=f'Name of the MySQL database (default: {DB_NAME})'
    )

    args = parser.parse_args()

    # --- INITIALIZATION ---
    # Configure logging based on the command-line argument
    # Log file name is now more generic
    logging.basicConfig(filename='dictionary_import.log', level=LOGGING_LEVELS[args.log_level],
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Open error log file - ensure it's closed in finally
    error_log = None
    try:
        error_log = open('import_error_summary.txt', 'w', encoding='utf-8') # More specific error summary file
    except IOError as e:
        logging.error(f"Failed to open error log file 'import_error_summary.txt': {e}")
        print(f"ERROR: Failed to open error log file 'import_error_summary.txt': {e}", file=sys.stderr)
        sys.exit(1)

    # --- DATABASE CONNECTION ---
    conn = None
    cursor = None
    if not args.dry_run:
        print(f"Attempting to connect to database '{args.db_name}' as user '{DB_USER}'...")
        try:
            conn = MySQLdb.connect(
                host=DB_HOST,
                user=DB_USER,
                passwd=DB_PASSWORD, # Use hardcoded password
                db=args.db_name,
                charset='utf8mb4',
                use_unicode=True, # Ensure data is treated as Unicode
                autocommit=False  # Explicit transaction control
            )
            cursor = conn.cursor()
            print(f"Successfully connected to database '{args.db_name}'.")
        except MySQLdb.Error as e:
            logging.error(f"DB Connection Failed: {e.args[0]}: {e.args[1]}")
            print(f"ERROR: DB Connection Failed: {e.args[0]}: {e.args[1]}", file=sys.stderr)
            print("Please check database connection details, ensure MySQL server is running,", file=sys.stderr)
            print(f"and that the user '{DB_USER}' has correct password and permissions on '{args.db_name}'.", file=sys.stderr)
            # Ensure error_log is closed before exiting
            if error_log:
                 error_log.close()
            sys.exit(1)
        except Exception as e:
            logging.error(f"An unexpected error occurred during database connection: {e}")
            print(f"ERROR: An unexpected error occurred during database connection: {e}", file=sys.stderr)
            # Ensure error_log is closed before exiting
            if error_log:
                 error_log.close()
            sys.exit(1)


    # --- PROCESSING SETUP ---
    word_cache = {}
    translation_batch = []
    # Updated stats names for clarity
    stats = {
        'processed_lines': 0,
        'errors': 0,
        'english_entries': 0,
        'english_entries_with_de_trans': 0, # New stat
        'english_entries_with_fa_trans': 0, # New stat
        'english_entries_with_both_de_fa_trans': 0, # Renamed stat
        'translation_pairs_batched': 0 # Pairs added to the batch list
    }

    try:
        # --- FILE PROCESSING LOOP ---
        print(f"Processing file: {args.jsonl_file}")
        # Check if the file exists before opening
        if not args.jsonl_file.is_file():
             logging.error(f"Input file not found at {args.jsonl_file}")
             print(f"ERROR: Input file not found at {args.jsonl_file}", file=sys.stderr)
             stats['errors'] += 1 # Count file not found as an error
             # No need to proceed if file not found
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
                    # Pass cursor explicitly as per your function definition
                    en_id = get_word_id(word_cache, english_word, "en", args.dry_run, cursor)
                    if en_id is None: # Use 'is None' to be explicit
                        logging.error(f"Failed to get/create word ID for English word '{english_word[:50]}...' on line {stats['processed_lines']}")
                        stats['errors'] += 1
                        error_log.write(f"L{stats['processed_lines']}_ERROR_GET_EN_ID: Failed to get/create ID for '{english_word}'.\nLine: {line[:200]}\n")
                        continue

                    # Extract translations for German and Persian from the English entry
                    translations = data.get("translations", [])
                    german_translations = [t for t in translations if t.get("code") == "de"] # Use "code" as per Wiktextract format
                    persian_translations = [t for t in translations if t.get("code") == "fa"] # Use "code" as per Wiktextract format

                    # --- FIX APPLIED HERE ---
                    # REMOVED: if not german_translations or not persian_translations: continue
                    # We now process DE and FA translations independently if they exist in the English entry.

                    if german_translations:
                         stats['english_entries_with_de_trans'] += 1
                         # Process German translations found in this English entry
                         for trans_entry in german_translations:
                             target_word_candidate = trans_entry.get("word") # Use "word" for the translation text/list
                             # Ensure target_words_text_list is a list of non-empty, stripped strings
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
                             elif target_word_candidate is not None: # Log if it was not None, but also not a processable type
                                  logging.warning(f"Unexpected 'word' data format for German translation in line {stats['processed_lines']} for source word '{english_word[:50]}...'. Data: '{target_word_candidate}'. Skipping translation entry.")
                                  error_log.write(f"L{stats['processed_lines']}_WARN_DE_TRANS_FORMAT: Unexpected target 'word' format for German translation of '{english_word}'. Data: '{target_word_candidate}'.\n")


                             # Add DE -> EN translation pairs (source=DE, target=EN)
                             if target_words_text_list_processed: # Check if any valid German words were found
                                 for de_word in target_words_text_list_processed:
                                     de_id = get_word_id(word_cache, de_word, "de", args.dry_run, cursor) # Pass cursor
                                     if de_id is not None:
                                         translation_batch.append((de_id, en_id))
                                         # stats['translation_pairs_batched'] += 1 # Count pairs added to batch - count moved to process_batch/flush_translation_batch

                    if persian_translations:
                         stats['english_entries_with_fa_trans'] += 1
                         # Process Persian translations found in this English entry
                         for trans_entry in persian_translations:
                             target_word_candidate = trans_entry.get("word") # Use "word" for the translation text/list
                             # Ensure target_words_text_list is a list of non-empty, stripped strings
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
                             elif target_word_candidate is not None: # Log if it was not None, but also not a processable type
                                  logging.warning(f"Unexpected 'word' data format for Persian translation in line {stats['processed_lines']} for source word '{english_word[:50]}...'. Data: '{target_word_candidate}'. Skipping translation entry.")
                                  error_log.write(f"L{stats['processed_lines']}_WARN_FA_TRANS_FORMAT: Unexpected target 'word' format for Persian translation of '{english_word}'. Data: '{target_word_candidate}'.\n")

                             # Add EN -> FA translation pairs (source=EN, target=FA)
                             if target_words_text_list_processed: # Check if any valid Persian words were found
                                 for fa_word in target_words_text_list_processed:
                                     fa_id = get_word_id(word_cache, fa_word, "fa", args.dry_run, cursor) # Pass cursor
                                     if fa_id is not None:
                                         translation_batch.append((en_id, fa_id))
                                         # stats['translation_pairs_batched'] += 1 # Count pairs added to batch - count moved to process_batch/flush_translation_batch

                    # Check if the English entry had both DE and FA translations listed
                    if german_translations and persian_translations:
                        stats['english_entries_with_both_de_fa_trans'] += 1


                    # Flush batch if size is reached
                    if len(translation_batch) >= TRANSLATION_BATCH_SIZE:
                        process_batch(conn, translation_batch, args.dry_run, cursor, stats) # Pass stats
                        # translation_batch is cleared inside process_batch


                except json.JSONDecodeError:
                    logging.warning(f"Skipping line {stats['processed_lines']} due to JSON parse error.")
                    stats['errors'] += 1
                    error_log.write(f"L{stats['processed_lines']}_WARN_JSON_ERROR: JSON parse error.\nLine: {line[:200]}\n")
                    continue
                except Exception as e:
                    logging.error(f"Unexpected error processing line {stats['processed_lines']}: {e}", exc_info=True)
                    stats['errors'] += 1
                    error_log.write(f"L{stats['processed_lines']}_ERROR_UNEXPECTED: Unexpected error: {e}\nLine: {line[:200]}\nTraceback: {sys.exc_info()[2]}\n") # Log traceback
                    continue

            # Process remaining batch after loop
            if translation_batch:
                process_batch(conn, translation_batch, args.dry_run, cursor, stats) # Pass stats

            # Create direct DE-FA links (only if not dry run)
            if not args.dry_run:
                 # Ensure cursor is available for create_direct_links
                 if conn and cursor:
                    create_direct_links(conn, cursor) # Pass cursor
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
        print(f"English entries with German translations found: {stats['english_entries_with_de_trans']}") # Updated stat name
        print(f"English entries with Persian translations found: {stats['english_entries_with_fa_trans']}") # Updated stat name
        print(f"English entries with both German and Persian translations listed: {stats['english_entries_with_both_de_fa_trans']}") # Updated stat name

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
        # Using a simple counter based on cache size + 1
        fake_id = len(cache) + 1
        cache[key] = fake_id
        return fake_id

    # Use the passed cursor for DB operations
    try:
        # Try to select the word first
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
                # Insert into translations table. source_id and target_id are foreign keys
                # to the words table.
                sql = "INSERT IGNORE INTO translations (source_id, target_id) VALUES (%s, %s)"
                # The translation_batch contains word IDs (integers).
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
    This query selects pairs of German and Persian word IDs that are both linked
    to the same English word ID through the translations table.
    """
    print("Creating direct DE-FA links via English bridge...")
    logging.info("Creating direct DE-FA links via English bridge...")
    try:
        # Use the passed cursor for DB operations
        sql = """
            INSERT IGNORE INTO translations (source_id, target_id)
            SELECT de_en.source_id, en_fa.target_id
            FROM translations de_en
            JOIN translations en_fa ON de_en.target_id = en_fa.source_id
            JOIN words de ON de_en.source_id = de.id AND de.lang_code = 'de'
            JOIN words en ON de_en.target_id = en.id AND en.lang_code = 'en'
            JOIN words fa ON en_fa.target_id = fa.id AND fa.lang_code = 'fa';
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
