## CS221 Project - Peterman Search Engine

This repository contains the starter skeleton code for the CS221 project. 

## Implement stemmers and tokenizers
- PunctuationTokenizer
- PorterStemmer(from Apache Lucene)
- WordBreakTokenizer

## Implement Inverted List and enable multiple search functions
### 1. Assumptions

- one single posting list **of a single keyword** could fit in memory.  
- one single posting list might span over multiple pages. 
- we don't assume all the posting lists of all keywords can fit into memory.

### 2. Data Structure Write into the disk
1. Every segment have a segment file, which contains the inverted lists information and a `db` file, which stores all documents in the corresponding segment.

2. For a segment file:
- Keyword dictionary: All the keywords in the segment are stored in the beginning of a segment, including the keyword, the page number and number of documents belonging to that keyword.
- After the keyword part, it is the posting lists of keywords. Every keyword will start a new page.(This makes recording the position of the posting lists simple, when the segment is very big, the waste of space can be ignored.)

3. For a documentStore file:
- We can create or open the documentStore file from `.db` file and do write or merge operations.