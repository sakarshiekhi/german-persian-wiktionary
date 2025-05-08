# Wiktextract Dictionary Importer

This script imports translation data from a Wiktextract JSONL dump into a MySQL database, focusing on creating translation links between German (de) and Persian (fa) words, bridged by English (en) entries. It finds English words that have translations in both German and Persian, and then creates direct DE-FA links in the database based on these findings.

## Features

* Processes Wiktextract JSONL data.
* Extracts English, German, and Persian words and their translations.
* Imports words and DE-EN, EN-FA translation pairs into a MySQL database.
* Creates direct DE-FA translation links based on the imported EN-bridged translations.
* Supports batch processing for efficient database inserts.
* Includes a dry-run mode to simulate the import without modifying the database.
* Configurable logging level.
* Supports specifying input file path, database name, host, user, and password via command-line arguments.

## Prerequisites

* Python 3.6+
* MySQL database server
* `mysqlclient` Python package (`pip install mysqlclient`)
* `tqdm` Python package (`pip install tqdm`)
* A Wiktextract data dump in JSONL format (e.g., `raw-wiktextract-data.jsonl`). You can typically download these from the [Wiktextract releases page](https://github.com/wiktextract/wiktextract/releases).

## Database Setup

Before running the script, you need to have a MySQL database created and a user with appropriate permissions to create tables and insert data.

The script expects two tables: `words` and `translations`. You can create them with SQL commands similar to these:

```sql
CREATE DATABASE IF NOT EXISTS dictdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE dictdb;

CREATE TABLE IF NOT EXISTS words (
    id INT AUTO_INCREMENT PRIMARY KEY,
    word_text VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    lang_code VARCHAR(10) NOT NULL,
    UNIQUE KEY unique_word_lang (word_text, lang_code)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS translations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_id INT NOT NULL,
    target_id INT NOT NULL,
    UNIQUE KEY unique_translation (source_id, target_id),
    FOREIGN KEY (source_id) REFERENCES words(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES words(id) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create indexes for faster lookups
CREATE INDEX idx_words_lang_word ON words (lang_code, word_text(100)); -- Index prefix for potentially long words
CREATE INDEX idx_translations_source_id ON translations (source_id);
CREATE INDEX idx_translations_target_id ON translations (target_id);
