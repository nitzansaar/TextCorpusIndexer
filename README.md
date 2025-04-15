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
- **Parallel execution using MapReduce**:
  - Dynamically adjusts mapper count based on input size
  - Customizable number of mappers and reducers
  - Efficient key partitioning for balanced load distribution

## Requirements

- Python 3.6 or higher
- mrjob library (for MapReduce implementation)

## Usage

To run the indexing system with the default data directory (`big_data`) and default parallelization:

```bash
python3 index.py big_data
```

To specify a different data directory and customize the parallelization:

```bash
python3 index.py path/to/data_directory --mappers 16 --reducers 8
```

Parameters:
- `data_directory`: Path to the directory containing your text data (required)
- `--mappers`: Number of mapper tasks to use for parallel processing (default: 8)
- `--reducers`: Number of reducer tasks to use for parallel processing (default: 4)

By default, the program expects a directory structure with:
- `data_directory/fulldata/`: Contains text files for unigram indexing
- `data_directory/devdata/`: Contains text files for bigram indexing

## Parallel Execution

The system implements parallel execution using MapReduce:

1. **Mapper Partitioning**: Input is partitioned by file, with each file processed by a separate mapper when possible
2. **Dynamic Scaling**: The number of mappers is automatically adjusted based on the input file count
3. **Key Partitioning**: A custom partitioner ensures even distribution of keys across reducers
4. **Configurable Parallelism**: The level of parallelism can be adjusted via command-line parameters

The parallel implementation significantly improves performance on large datasets by distributing the workload across multiple processors.

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

If errors occur, appropriate error messages will be displayed.
