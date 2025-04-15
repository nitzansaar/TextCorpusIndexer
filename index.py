#!/usr/bin/env python3
import os
import re
import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
import argparse

class TextFileIndexer:
    def __init__(self, data_dir="big_data", num_mappers=8, num_reducers=4):
        """Initialize the indexer with the data directory path and parallelization settings."""
        self.data_dir = data_dir
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
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
        print("Starting Text File Indexing System with Parallel MapReduce...")
        print(f"Using parallel execution with multiple mappers and reducers")
        
        # Create unigram index
        print("\nCreating unigram index from fulldata directory in parallel...")
        self.run_unigram_job()
        
        # Create bigram index
        print("\nCreating selective bigram index from devdata directory in parallel...")
        self.run_bigram_job()
        
        print("\nParallel MapReduce indexing completed successfully!")
    
    def run_unigram_job(self):
        fulldata_dir = os.path.join(self.data_dir, "fulldata")
        input_files = []
        for filename in os.listdir(fulldata_dir):
            if filename.endswith('.txt'):
                input_files.append(os.path.join(fulldata_dir, filename))
        
        # Dynamically adjust mapper count based on input size
        dynamic_mapper_count = min(len(input_files), self.num_mappers)
        # Ensure at least one mapper per file, up to num_mappers
        mapper_count = max(1, dynamic_mapper_count)
        
        # Run the MapReduce job with parallel configuration
        mr_job = UnigramMRJob(args=input_files + [
            '-o', 'unigram_output',
            '--num-mappers', str(mapper_count),
            '--num-reducers', str(self.num_reducers)
        ])
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
        
        # Dynamically adjust mapper count based on input size
        dynamic_mapper_count = min(len(input_files), self.num_mappers)
        # Ensure at least one mapper per file, up to num_mappers
        mapper_count = max(1, dynamic_mapper_count)
        
        # Run the MapReduce job with parallel configuration
        mr_job = BigramMRJob(args=input_files + [
            '-o', 'bigram_output', 
            '--selected-bigrams'] + self.selected_bigrams + [
            '--num-mappers', str(mapper_count),
            '--num-reducers', str(self.num_reducers)
        ])
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
    
    def configure_args(self):
        """Add custom command line options."""
        super(UnigramMRJob, self).configure_args()
        self.add_passthru_arg('--num-mappers', dest='num_mappers', 
                            type=int, default=4, help='Number of mappers to use')
        self.add_passthru_arg('--num-reducers', dest='num_reducers', 
                            type=int, default=4, help='Number of reducers to use')
    
    def configure_options(self):
        """Configure Hadoop job options for parallelization."""
        super(UnigramMRJob, self).configure_options()
        self.pass_through_option('--jobconf', dest='jobconf', 
                                default=[], action='append',
                                help='Specify jobconf arguments to pass to Hadoop streaming')
    
    def jobconf(self):
        """Set Hadoop job configuration for parallelization."""
        conf = {
            'mapreduce.job.maps': self.options.num_mappers,
            'mapreduce.job.reduces': self.options.num_reducers,
            'mapreduce.partition.keypartitioner.options': '-k1,1',
            'stream.num.map.output.key.fields': '1',
            'mapreduce.job.partitioner.class': 'org.apache.hadoop.mapred.lib.HashPartitioner',
        }
        
        # Get the parent's jobconf
        parent_jobconf = super(UnigramMRJob, self).jobconf()
        
        # Update with our configuration
        parent_jobconf.update(conf)
        return parent_jobconf
    
    @staticmethod
    def partitioner():
        """Return a partitioner function that distributes keys evenly."""
        def partition_func(key, num_reducers):
            """Distribute keys evenly across reducers based on hash of the key."""
            # Simple hash-based partitioner that ensures words with similar 
            # first letters are distributed across reducers
            if not key:
                return 0
            return hash(key[0]) % num_reducers
        return partition_func
    
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
        self.add_passthru_arg('--num-mappers', dest='num_mappers', 
                            type=int, default=4, help='Number of mappers to use')
        self.add_passthru_arg('--num-reducers', dest='num_reducers', 
                            type=int, default=4, help='Number of reducers to use')
    
    def configure_options(self):
        """Configure Hadoop job options for parallelization."""
        super(BigramMRJob, self).configure_options()
        self.pass_through_option('--jobconf', dest='jobconf', 
                                default=[], action='append',
                                help='Specify jobconf arguments to pass to Hadoop streaming')
    
    def jobconf(self):
        """Set Hadoop job configuration for parallelization."""
        conf = {
            'mapreduce.job.maps': self.options.num_mappers,
            'mapreduce.job.reduces': self.options.num_reducers,
            'mapreduce.partition.keypartitioner.options': '-k1,1',
            'stream.num.map.output.key.fields': '1',
            'mapreduce.job.partitioner.class': 'org.apache.hadoop.mapred.lib.HashPartitioner',
        }
        
        # Get the parent's jobconf
        parent_jobconf = super(BigramMRJob, self).jobconf()
        
        # Update with our configuration
        parent_jobconf.update(conf)
        return parent_jobconf
    
    @staticmethod
    def partitioner():
        """Return a partitioner function that distributes keys evenly."""
        def partition_func(key, num_reducers):
            """Distribute keys evenly across reducers based on hash of the key."""
            # Simple hash-based partitioner that ensures bigrams with similar 
            # first words are distributed across reducers
            if not key:
                return 0
            words = key.split(' ', 1)
            return hash(words[0]) % num_reducers
        return partition_func
    
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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run MapReduce text indexing')
    parser.add_argument('data_dir', help='Directory containing the text data')
    parser.add_argument('--mappers', type=int, default=8, 
                      help='Number of mapper tasks (default: 8)')
    parser.add_argument('--reducers', type=int, default=4, 
                      help='Number of reducer tasks (default: 4)')
    
    args = parser.parse_args()
    
    print(f"Running with {args.mappers} mappers and {args.reducers} reducers")
    
    # Create and run the indexer with specified parallelization
    indexer = TextFileIndexer(args.data_dir, args.mappers, args.reducers)
    indexer.run() 