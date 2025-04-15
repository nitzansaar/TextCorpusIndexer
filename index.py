#!/usr/bin/env python3
import os
import re
import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict

class TextFileIndexer:
    def __init__(self, data_dir="big_data"):
        """Initialize the indexer with the data directory path."""
        self.data_dir = data_dir
        # List of specific bigrams to track
        self.selected_bigrams = [
            "computer science",
            "information retrieval",
            "power politics",
            "los angeles",
            "bruce willis"
        ]
    
    def normalize_word(self, word):
        """Normalize words by converting to lowercase and replacing punctuation and numerals with spaces."""
        # Convert to lowercase
        word = word.lower()
        # Replace punctuation and numerals with spaces
        word = re.sub(r'[^\w\s]|[\d]', ' ', word)
        # Collapse multiple spaces into a single space
        word = re.sub(r'\s+', ' ', word)
        return word.strip()
    
    def run(self):
        """Run the full indexing process."""
        print("Starting Text File Indexing System with MapReduce...")
        
        # Create unigram index
        print("\nCreating unigram index from fulldata directory...")
        self.run_unigram_job()
        
        # Create bigram index
        print("\nCreating selective bigram index from devdata directory...")
        self.run_bigram_job()
        
        print("\nMapReduce indexing completed successfully!")
    
    def run_unigram_job(self):
        fulldata_dir = os.path.join(self.data_dir, "fulldata")
        input_files = []
        for filename in os.listdir(fulldata_dir):
            if filename.endswith('.txt'):
                input_files.append(os.path.join(fulldata_dir, filename))
        
        # Run the MapReduce job
        mr_job = UnigramMRJob(args=input_files + ['-o', 'unigram_output'])
        with mr_job.make_runner() as runner:
            runner.run()
            
            # Extract output and write to file
            with open('unigram_index.txt', 'w') as output_file:
                for line in runner.cat_output():
                    # Process the line
                    line_str = line.decode('utf-8')
                    if line_str.startswith('null\t'):
                        line_str = line_str[5:]
                    # Remove quotes if present
                    line_str = line_str.strip()
                    if line_str.startswith('"') and line_str.endswith('"'):
                        line_str = line_str[1:-1]
                    output_file.write(f"{line_str}\n")
        
        print(f"Unigram index written to unigram_index.txt")
    
    def run_bigram_job(self):
        devdata_dir = os.path.join(self.data_dir, "devdata")
        input_files = []
        for filename in os.listdir(devdata_dir):
            if filename.endswith('.txt'):
                input_files.append(os.path.join(devdata_dir, filename))
        
        # Run the MapReduce job
        mr_job = BigramMRJob(args=input_files + 
                            ['-o', 'bigram_output', 
                            '--selected-bigrams'] + self.selected_bigrams)
        with mr_job.make_runner() as runner:
            runner.run()
            
            # Extract output and write to file
            with open('selected_bigram_index.txt', 'w') as output_file:
                for line in runner.cat_output():
                    # Process the line
                    line_str = line.decode('utf-8')
                    if line_str.startswith('null\t'):
                        line_str = line_str[5:]
                    # Remove quotes if present
                    line_str = line_str.strip()
                    if line_str.startswith('"') and line_str.endswith('"'):
                        line_str = line_str[1:-1]
                    output_file.write(f"{line_str}\n")
        
        print(f"Bigram index written to selected_bigram_index.txt")

class UnigramMRJob(MRJob):
    """MapReduce job for creating unigram index."""
    
    def mapper_init(self):
        """Initialize the mapper with helper functions."""
        self.normalize_word = lambda word: re.sub(r'\s+', ' ', re.sub(r'[^\w\s]|[\d]', ' ', word.lower())).strip()
        self.tokenize_text = lambda text: [token for token in self.normalize_word(text).split() if token.strip()]
    
    def mapper(self, _, line):
        """Map function to process each line (document)."""
        parts = line.split('\t', 1)
        if len(parts) != 2:
            return
        
        doc_id = parts[0].strip()
        text = parts[1].strip()
        tokens = self.tokenize_text(text)
        
        # Emit (word, (doc_id, count)) for each word
        word_counts = defaultdict(int)
        for token in tokens:
            if token:  # Skip empty tokens
                word_counts[token] += 1
        
        for word, count in word_counts.items():
            yield word, (doc_id, count)
    
    def reducer(self, word, doc_counts):
        """Reduce function to aggregate document counts for each word."""
        # Combine all document counts for the word
        result = []
        doc_count_map = defaultdict(int)
        
        for doc_id, count in doc_counts:
            doc_count_map[doc_id] += count
        
        # Sort by document ID
        for doc_id in sorted(doc_count_map.keys()):
            result.append(f"{doc_id}:{doc_count_map[doc_id]}")
        
        output_line = f"{word}, {' '.join(result)}"
        yield None, output_line
    
    def reducer_final(self, word, values):
        """Final reducer to format the output."""
        for value in values:
            yield None, value
    
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_final
            )
        ]

class BigramMRJob(MRJob):
    """MapReduce job for creating bigram index."""
    
    def configure_args(self):
        """Add custom command line options."""
        super(BigramMRJob, self).configure_args()
        self.add_passthru_arg('--selected-bigrams', dest='selected_bigrams', 
                            nargs='+', default=[])
    
    def mapper_init(self):
        """Initialize the mapper with helper functions and selected bigrams."""
        self.normalize_word = lambda word: re.sub(r'\s+', ' ', re.sub(r'[^\w\s]|[\d]', ' ', word.lower())).strip()
        self.tokenize_text = lambda text: [token for token in self.normalize_word(text).split() if token.strip()]
        self.selected_bigrams = set(self.options.selected_bigrams)
    
    def mapper(self, _, line):
        """Map function to process each line (document)."""
        parts = line.split('\t', 1)
        if len(parts) != 2:
            return
        
        doc_id = parts[0].strip()
        text = parts[1].strip()
        tokens = self.tokenize_text(text)
        
        # Process bigrams
        for i in range(len(tokens) - 1):
            if tokens[i] and tokens[i+1]:  # Skip empty tokens
                bigram = f"{tokens[i]} {tokens[i+1]}"
                # Only add bigram if it's in our selected list
                if bigram in self.selected_bigrams:
                    yield bigram, (doc_id, 1)
    
    def combiner(self, bigram, doc_counts):
        """Combine values for the same key (bigram, doc_id) from the same mapper."""
        # Combine counts for the same document
        doc_count_map = defaultdict(int)
        for doc_id, count in doc_counts:
            doc_count_map[doc_id] += count
        
        # Emit combined counts
        for doc_id, count in doc_count_map.items():
            yield bigram, (doc_id, count)
    
    def reducer(self, bigram, doc_counts):
        """Reduce function to aggregate document counts for each bigram."""
        # Combine all document counts for the bigram
        doc_count_map = defaultdict(int)
        
        for doc_id, count in doc_counts:
            doc_count_map[doc_id] += count
        
        # Sort by document ID and format the output
        result = []
        for doc_id in sorted(doc_count_map.keys()):
            result.append(f"{doc_id}:{doc_count_map[doc_id]}")
        
        output_line = f"{bigram} {' '.join(result)}"
        yield None, output_line
    
    def reducer_final(self, bigram, values):
        """Final reducer to format the output."""
        for value in values:
            yield None, value
    
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_final
            )
        ]

if __name__ == "__main__":
    # Use command line argument for data_dir if provided
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "big_data"
    
    indexer = TextFileIndexer(data_dir)
    indexer.run() 