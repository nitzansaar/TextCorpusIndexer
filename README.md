# Text File Indexing System

This is a document processing application that creates word indexes from collections of text files. The system processes text files from specified directories and generates index files according to defined formats.

## Features

- Creates a unigram index from text files in the 'fulldata' directory
- Creates a selective bigram index from text files in the 'devdata' directory
- Normalizes text by:
  - Converting all words to lowercase
  - Replacing all punctuation and numerals with spaces
  - Properly handling tab characters in the document content
- Generates properly formatted output files

## Requirements

- Python 3.6 or higher

## Usage

To run the indexing system with the default data directory (`big_data`):

```bash
python3 index.py
```

To specify a different data directory:

```bash
python3 index.py path/to/data_directory
```

By default, the program expects a directory structure with:
- `data_directory/fulldata/`: Contains text files for unigram indexing
- `data_directory/devdata/`: Contains text files for bigram indexing

## Input Format

The input text files should have the following format:
- Each file begins with a document ID followed by a tab character and then the document content
- Document content may contain additional tab characters which are handled by the system
- Example: `5722018435[TAB]We reject this argument because section 5340...`

## Text Processing

The system performs the following text processing:
1. Separates the document ID (key) from content (value) using the first tab character
2. Converts all text to lowercase
3. Replaces all punctuation and numerals with spaces
4. Normalizes spacing (collapses multiple spaces into single spaces)
5. Tokenizes the text into words for indexing

## Output Format

The program generates two output files:

1. `unigram_index.txt`: Contains each word followed by document ID and count pairs
   - Format: `word, docID:count [docID:count]...`
   - Example: `argument, 5722018435:1 5722018440:3`

2. `selected_bigram_index.txt`: Contains each bigram followed by document ID
   - Format: `word1 word2, docID`
   - Example: `we reject, 5722018435`

## Error Handling

The program handles the following error conditions:
- Missing directories
- Malformed input files
- File access issues

If errors occur, appropriate error messages will be displayed. # TextCorpusIndexer
