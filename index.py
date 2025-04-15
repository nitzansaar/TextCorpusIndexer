#!/usr/bin/env python3
import os
import re
import sys
from collections import defaultdict

class TextFileIndexer:
    def __init__(self, data_dir="big_data"):
        """Initialize the indexer with the data directory path."""
        self.data_dir = data_dir
        self.unigram_index = defaultdict(lambda: defaultdict(int))
        self.bigram_index = defaultdict(lambda: defaultdict(int))
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
    
    def tokenize_text(self, text):
        """Tokenize text into words."""
        # Normalize the entire text first
        normalized_text = self.normalize_word(text)
        # Split by whitespace and filter out empty tokens
        return [token for token in normalized_text.split() if token.strip()]
    
    def process_file(self, file_path, index_type):
        """Process a single file and update the appropriate index."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read().strip()
                
                # Extract document ID (first token before tab)
                parts = content.split('\t', 1)
                if len(parts) != 2:
                    print(f"Warning: File {file_path} does not have the expected format.")
                    return
                
                doc_id = parts[0].strip()
                text = parts[1].strip()
                tokens = self.tokenize_text(text)
                
                if index_type == "unigram":
                    # Process for unigram index
                    for token in tokens:
                        if token:  # Skip empty tokens
                            self.unigram_index[token][doc_id] += 1
                
                elif index_type == "bigram":
                    # Process for bigram index
                    for i in range(len(tokens) - 1):
                        if tokens[i] and tokens[i+1]:  # Skip empty tokens
                            bigram = f"{tokens[i]} {tokens[i+1]}"
                            # Only add bigram if it's in our selected list
                            if bigram in self.selected_bigrams:
                                self.bigram_index[bigram][doc_id] += 1
        
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
    
    def process_directory(self, directory, index_type):
        """Process all files in the specified directory."""
        if not os.path.exists(directory):
            print(f"Error: Directory {directory} does not exist.")
            return
        
        file_count = 0
        for filename in os.listdir(directory):
            if filename.endswith(".txt"):
                file_path = os.path.join(directory, filename)
                self.process_file(file_path, index_type)
                file_count += 1
        
        print(f"Processed {file_count} files from {directory}")
    
    def create_unigram_index(self):
        """Create unigram index from files in the fulldata directory."""
        fulldata_dir = os.path.join(self.data_dir, "fulldata")
        self.process_directory(fulldata_dir, "unigram")
    
    def create_bigram_index(self):
        """Create bigram index from files in the devdata directory."""
        devdata_dir = os.path.join(self.data_dir, "devdata")
        self.process_directory(devdata_dir, "bigram")
    
    def write_unigram_index(self, output_file="unigram_index.txt"):
        """Write the unigram index to a file."""
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                # Sort words alphabetically
                sorted_words = sorted(self.unigram_index.keys())
                
                for word in sorted_words:
                    # Format: word, docID:count [docID:count]...
                    doc_counts = []
                    for doc_id, count in sorted(self.unigram_index[word].items()):
                        doc_counts.append(f"{doc_id}:{count}")
                    
                    line = f"{word}, {' '.join(doc_counts)}"
                    file.write(line + "\n")
            
            print(f"Unigram index written to {output_file}")
        
        except Exception as e:
            print(f"Error writing unigram index to {output_file}: {e}")
    
    def write_bigram_index(self, output_file="selected_bigram_index.txt"):
        """Write the bigram index to a file."""
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                # Sort bigrams alphabetically
                sorted_bigrams = sorted(self.bigram_index.keys())
                
                for bigram in sorted_bigrams:
                    # Format: bigram docID:count docID:count...
                    doc_counts = []
                    for doc_id, count in sorted(self.bigram_index[bigram].items()):
                        doc_counts.append(f"{doc_id}:{count}")
                    
                    line = f"{bigram} {' '.join(doc_counts)}"
                    file.write(line + "\n")
            
            print(f"Bigram index written to {output_file}")
        
        except Exception as e:
            print(f"Error writing bigram index to {output_file}: {e}")
    
    def run(self):
        """Run the full indexing process."""
        print("Starting Text File Indexing System...")
        
        # Create unigram index
        print("\nCreating unigram index from fulldata directory...")
        self.create_unigram_index()
        self.write_unigram_index()
        
        # Create bigram index
        print("\nCreating selective bigram index from devdata directory...")
        self.create_bigram_index()
        self.write_bigram_index()
        
        print("\nIndexing completed successfully!")

if __name__ == "__main__":
    # Use command line argument for data_dir if provided
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "big_data"
    
    indexer = TextFileIndexer(data_dir)
    indexer.run() 