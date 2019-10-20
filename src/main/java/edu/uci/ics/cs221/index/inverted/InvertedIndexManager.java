package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 *
 * Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
     *
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;

    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     *
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    private String currentFolder;

    // this is a count value for segment number, need to increment after each flush
    private int SegmentNum;

    private LinkedHashMap<String, List<Integer>> invertedMap = new LinkedHashMap<>();
    private LinkedHashMap<Integer, Document> documentMap = new LinkedHashMap<>();
    private Table<String, Integer, List<Integer>> positions = HashBasedTable.create();

    private Analyzer iAnalyzer;
    private Compressor iCompressor;
    private PriorityQueue<DocID> topKDocList = new PriorityQueue<>(Comparator.reverseOrder());

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        currentFolder = indexFolder;
        iAnalyzer = analyzer;
        iCompressor = new DeltaVarLenCompressor();
        SegmentNum = 0;
    }

    private InvertedIndexManager(String indexFolder, Analyzer positionalAnalyzer, Compressor compressor) {
        currentFolder = indexFolder;
        iAnalyzer = positionalAnalyzer;
        iCompressor = compressor;
        SegmentNum = 0;
    }

    /**
     * Inner class used to store key information
     */
    class KeyInfo {
        int segmentNumber;
        int invertedListOffset;
        int invertedDocIDCompressedLen;
        int invertedOffsetCompressedLen;

        public KeyInfo(int segmentNumber, int invertedListOffset, int invertedDocIDCompressedLen, int invertedOffsetCompressedLen) {
            this.segmentNumber = segmentNumber;
            this.invertedListOffset = invertedListOffset;
            this.invertedDocIDCompressedLen = invertedDocIDCompressedLen;
            this.invertedOffsetCompressedLen = invertedOffsetCompressedLen;
        }
    }

    class PositionalInfo {
        int docID;
        int segmentIdentifier;
        int positionalOffset;
        int positionalLength;

        public PositionalInfo(int docID, int segmentIdentifier, int positionalOffset, int positionalLength) {
            this.docID = docID;
            this.segmentIdentifier = segmentIdentifier;
            this.positionalOffset = positionalOffset;
            this.positionalLength = positionalLength;
        }
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a positional index with the given folder, analyzer, and the compressor.
     * Compressor must be used to compress the inverted lists and the position lists.
     *
     */
    public static InvertedIndexManager createOrOpenPositional(String indexFolder, Analyzer analyzer, Compressor compressor) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer, compressor);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer, compressor);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     * @param document
     */
    public void addDocument(Document document) {

        // add to document list
        int docID = documentMap.size();
        documentMap.put(docID, document);

        // add to inverted list and table
        String text = document.getText();
        List<String> words = iAnalyzer.analyze(text);
        int counter = 0;
        for (String word : words) {
            if (invertedMap.containsKey(word)) {
                if (!invertedMap.get(word).contains(docID)) {
                    invertedMap.get(word).add(docID);
                }
            } else {
                invertedMap.put(word,new ArrayList<>());
                invertedMap.get(word).add(docID);
            }

            if (positions.contains(word, docID)) {
                positions.get(word, docID).add(counter);
            } else {
                List<Integer> list = new ArrayList<>();
                list.add(counter);
                positions.put(word, docID, list);
            }
            counter++;
        }

        // when reach the threshold, flush
        if (documentMap.size() == DEFAULT_FLUSH_THRESHOLD) {
            flush();
        }
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        /**
         * The documents themselves are stored in DocumentStore.
         * When you do `flush()`, you should
         * 1) write your dictionary and inverted lists (which has document IDs),
         * and 2) create the DocumentStore with `<DocID, Document>` pairs.
         */
        if (documentMap.size() == 0) return;

        /// create the document store
        String dirPath = currentFolder + File.separator + "Mapdb" + SegmentNum + ".db";
        DocumentStore storage = MapdbDocStore.createOrOpen(dirPath);

        // add all <DocID, Document> pairs in the memory to the documentStore
        for (Map.Entry<Integer, Document> entry : documentMap.entrySet()) {
            storage.addDocument(entry.getKey(), entry.getValue());
        }

        storage.close(); // after add operations, close the storage

        // write the dictionary
        Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionary" + SegmentNum);
        PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);

        // write the inverted lists
        Path invertedListPath = Paths.get(currentFolder + File.separator + "invertedList" + SegmentNum);
        PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListPath);

        // write the positional lists
        Path positionalListPath = Paths.get(currentFolder + File.separator + "positionalList" + SegmentNum);
        PageFileChannel positionalListPageFileChannel = PageFileChannel.createOrOpen(positionalListPath);
        writeInvertedListToDiskForInitialSegment(invertedMap, positions, dictionaryPageFileChannel,
                invertedListPageFileChannel, positionalListPageFileChannel);

        // clear the memory after flushing
        invertedMap.clear();
        documentMap.clear();
        positions.clear();

        SegmentNum++;
        // merge if reach the threshold
        if (getNumSegments() == DEFAULT_MERGE_THRESHOLD) {
            mergeAllSegments();
        }
    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);

        for (int i = 0; i < SegmentNum; i += 2) {
            // get the offset for doc IDs in the merged segment
            String dbFilePath = currentFolder + File.separator + "Mapdb" + i + ".db";
            DocumentStore mapDB = MapdbDocStore.createOrOpen(dbFilePath);
            int offset = (int) mapDB.size();
            mapDB.close();

            int secondSegmentNum = i + 1;
            String previousDictionary1 = "dictionary" + i;
            String previousDictionary2 = "dictionary" + secondSegmentNum;

            String previousInvertedList1 = "invertedList" + i;
            String previousInvertedList2 = "invertedList" + secondSegmentNum;

            String previousPositionalList1 = "positionalList" + i;
            String previousPositionalList2 = "positionalList" + secondSegmentNum;

            // give it a new name, segmentx and rename it after the deletion of the previous segments
            int currentSegmentNumber = i / 2;
            Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionaryx");
            PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);

            Path invertedListFilePath = Paths.get(currentFolder + File.separator + "invertedListx");
            PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListFilePath);

            Path positionalListFilePath = Paths.get(currentFolder + File.separator + "positionalListx");
            PageFileChannel positionalListPageFileChannel = PageFileChannel.createOrOpen(positionalListFilePath);

            // construct and write merged information to disk
            writeInvertedListToDiskForMergedSegment(i, offset, dictionaryPageFileChannel, invertedListPageFileChannel, positionalListPageFileChannel);

            try {
                File folder = new File(currentFolder);
                String[] entries = folder.list();
                for (String s : entries) {
                    if (s.endsWith(previousDictionary1) || s.endsWith(previousDictionary2)
                            || s.endsWith(previousInvertedList1) || s.endsWith(previousInvertedList2)
                            || s.endsWith(previousPositionalList1) || s.endsWith(previousPositionalList2)) {
                        File currentFile = new File(folder.getPath(), s);
                        currentFile.delete();
                    }
                }
            } catch (Exception e) {
                System.out.println("Something went wrong when deleting file");
            }

            // rename new files
            File oldDictionary = new File(currentFolder + File.separator + "dictionaryx");
            File newDictionary = new File(currentFolder + File.separator + "dictionary" + currentSegmentNumber);
            oldDictionary.renameTo(newDictionary);

            File oldInvertedList = new File(currentFolder + File.separator + "invertedListx");
            File newInvertedList = new File(currentFolder + File.separator + "invertedList" + currentSegmentNumber);
            oldInvertedList.renameTo(newInvertedList);

            File oldPositionalList = new File(currentFolder + File.separator + "positionalListx");
            File newPositionalList = new File(currentFolder + File.separator + "positionalList" + currentSegmentNumber);
            oldPositionalList.renameTo(newPositionalList);


            // merge documentStore
            String dbFilePath1 = currentFolder + File.separator + "Mapdb" + i + ".db";
            DocumentStore documentStore1 = MapdbDocStore.createOrOpen(dbFilePath1);
            String dbFilePath2 = currentFolder + File.separator + "Mapdb" + secondSegmentNum + ".db";
            DocumentStore documentStore2 = MapdbDocStore.createOrOpen(dbFilePath2);

            // iterate documentStore2 and put it into documentStore1
            Iterator<Map.Entry<Integer, Document>> docStore2Iterator = documentStore2.iterator();
            int counter = 0;
            while (docStore2Iterator.hasNext()) {
                Map.Entry<Integer, Document> entry = docStore2Iterator.next();
                documentStore1.addDocument(counter + offset, entry.getValue());
                counter++;
            }

            documentStore1.close();
            documentStore2.close();

            // rename documentStore1
            int currentMapDBNumber = i / 2;
            File db1 = new File(dbFilePath1);
            File db2 = new File(currentFolder + File.separator + "Mapdb" + currentMapDBNumber + ".db");
            db1.renameTo(db2);

            // delete currentFolder + File.separator + "Mapdb" + secondSegmentNum + ".db"
            String docStore2 = currentFolder + File.separator + "Mapdb" + secondSegmentNum + ".db";
            try {
                Files.deleteIfExists(Paths.get(docStore2));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // update new segment number
        SegmentNum = (SegmentNum + 1) / 2;
    }

    /**
     * Performs a single keyword search on the inverted index.
     * You could assume the analyzer won't convert the keyword into multiple tokens.
     * If the keyword is empty, it should not return anything.
     *
     * @param keyword keyword, cannot be null.
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchQuery(String keyword) {
        Preconditions.checkNotNull(keyword);
        // assume the analyzer won't convert the keyword into multiple tokens
        List<String> words = iAnalyzer.analyze(keyword);
        keyword = words.get(0);

        List<Document> documentList = new ArrayList<>();
        if (keyword.length() == 0) {
            return documentList.iterator();
        }

        // check if the existing segments has the keyword
        for (int i = 0; i < SegmentNum; i++) {
            Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionary" + i);
            PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);
            Path invertedListFilePath = Paths.get(currentFolder + File.separator + "invertedList" + i);
            PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListFilePath);

            LinkedHashMap<String, KeyInfo> keywordList = new LinkedHashMap<>();
            constructKeywordList(keywordList, dictionaryPageFileChannel, i);

            if (keywordList.containsKey(keyword)) {
                String dbFilePath = currentFolder + File.separator + "Mapdb" + i + ".db";
                DocumentStore mapDB = MapdbDocStore.createOrOpen(dbFilePath);

                KeyInfo keyInfo = keywordList.get(keyword);

                int invertedListOffset = keyInfo.invertedListOffset;
                int invertedDocIDCompressedLen = keyInfo.invertedDocIDCompressedLen;

                byte[] invertedDocIDListBytes = readBytesWithCertainOffsetAndLength(invertedListPageFileChannel, invertedListOffset, invertedDocIDCompressedLen);
                List<Integer> invertedDocIDList = iCompressor.decode(invertedDocIDListBytes);
                for (int j = 0; j < invertedDocIDList.size(); j++) {
                    documentList.add(mapDB.getDocument(invertedDocIDList.get(j)));
                }
                mapDB.close();
            }
            dictionaryPageFileChannel.close();
            invertedListPageFileChannel.close();
        }

        return documentList.iterator();
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        // assume the analyzer won't convert the keyword into multiple tokens.
        keywords = analyzeKeywords(keywords);
        List<Document> documentList = new ArrayList<>();
        if (keywords.size() == 0) {
            return documentList.iterator();
        } else if (keywords.size() == 1) {
            return searchQuery(keywords.get(0));
        }
        int len = keywords.size();

        for (int i = 0; i < SegmentNum; i++) {
            List<Integer> currentSegmentList = searchKeywordInSingleSegment(keywords.get(0), i);
            // traverse keywords
            for (int j = 0; j < len - 1; j++) {
                List<Integer> secondList = searchKeywordInSingleSegment(keywords.get(j + 1), i);
                currentSegmentList = mergeAndKeywordID(currentSegmentList, secondList);
                if (currentSegmentList.size() == 0) {
                    break;
                }
            }
            String dbFilePath = currentFolder + File.separator + "Mapdb" + i + ".db";
            DocumentStore documentStore = MapdbDocStore.createOrOpen(dbFilePath);
            for (Integer docID : currentSegmentList) {
                Document currentDocument = documentStore.getDocument(docID);
                if (currentDocument != null) {
                    documentList.add(currentDocument);
                }
            }
            documentStore.close();
        }
        return documentList.iterator();
    }

    private List<String> analyzeKeywords(List<String> keywords) {
        List<String> realKeywords = new ArrayList<>();
        for (String keyword : keywords) {
            List<String> currentKeyword = iAnalyzer.analyze(keyword);
            if (currentKeyword.size() > 0) {
                realKeywords.add(currentKeyword.get(0));
            }
        }
        return realKeywords;
    }

    private List<Integer> mergeAndKeywordID(List<Integer> first, List<Integer> second) {
        List<Integer> result = new ArrayList<>();
        if (first.size() == 0 || second.size() == 0) {
            return result;
        }
        int firstPointer = 0;
        int secondPointer = 0;
        while (firstPointer < first.size() && secondPointer < second.size()) {
            int docID1 = first.get(firstPointer);
            int docID2 = second.get(secondPointer);
            if (docID1 == docID2) {
                result.add(docID1);
                firstPointer++;
                secondPointer++;
            } else if (docID1 < docID2) {
                firstPointer++;
            } else {
                secondPointer++;
            }
        }
        return result;
    }

    /**
     * @param keyword
     * @param segmentNumber
     * @return a posting list for a keyword in a specific segment OR NULL if the keyword doesn't exist
     */
    private List<Integer> searchKeywordInSingleSegment(String keyword, int segmentNumber) {
        List<Integer> postingList = new ArrayList<>();

        Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionary" + segmentNumber);
        PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);

        Path invertedListFilePath = Paths.get(currentFolder + File.separator + "invertedList" + segmentNumber);
        PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListFilePath);

        LinkedHashMap<String, KeyInfo> keywordList = new LinkedHashMap<>();
        constructKeywordList(keywordList, dictionaryPageFileChannel, segmentNumber);

        if (keywordList.containsKey(keyword)) {
            KeyInfo keyInfo = keywordList.get(keyword);
            int invertedListOffset = keyInfo.invertedListOffset;
            int invertedDocIDCompressedLen = keyInfo.invertedDocIDCompressedLen;

            byte[] invertedDocIDListBytes = readBytesWithCertainOffsetAndLength(invertedListPageFileChannel, invertedListOffset, invertedDocIDCompressedLen);
            List<Integer> invertedDocIDList = iCompressor.decode(invertedDocIDListBytes);
            for (int j = 0; j < invertedDocIDList.size(); j++) {
                postingList.add(invertedDocIDList.get(j));
            }
        }
        dictionaryPageFileChannel.close();
        invertedListPageFileChannel.close();

        return postingList;
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        keywords = analyzeKeywords(keywords);
        List<Document> documentList = new ArrayList<>();
        if (keywords.size() == 0) {
            return documentList.iterator();
        } else if (keywords.size() == 1) {
            return searchQuery(keywords.get(0));
        }
        int len = keywords.size();

        for (int i = 0; i < SegmentNum; i++) {
            List<Integer> currentSegmentList = searchKeywordInSingleSegment(keywords.get(0), i);
            // traverse keywords
            for (int j = 0; j < len - 1; j++) {
                List<Integer> secondList = searchKeywordInSingleSegment(keywords.get(j + 1), i);
                currentSegmentList = mergeOrKeywordID(currentSegmentList, secondList);
            }
            String dbFilePath = currentFolder + File.separator + "Mapdb" + i + ".db";
            DocumentStore documentStore = MapdbDocStore.createOrOpen(dbFilePath);
            for (Integer docID : currentSegmentList) {
                documentList.add(documentStore.getDocument(docID));
            }
            documentStore.close();
        }
        return documentList.iterator();
    }

    private List<Integer> mergeOrKeywordID(List<Integer> first, List<Integer> second) {
        List<Integer> result = new ArrayList<>();
        if (first.size() == 0) {
            return second;
        } else if (second.size() == 0) {
            return first;
        }
        for (Integer i : first) {
            result.add(i);
        }
        for (Integer i : second) {
            if (!result.contains(i)) {
                result.add(i);
            }
        }
        Collections.sort(result);
        return result;
    }

    /**
     * Performs a phrase search on a positional index.
     * Phrase search means the document must contain the consecutive sequence of keywords in exact order.
     *
     * You could assume the analyzer won't convert each keyword into multiple tokens.
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param phrase, a consecutive sequence of keywords
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchPhraseQuery(List<String> phrase) {
        Preconditions.checkNotNull(phrase);
        List<Document> documentList = new ArrayList<>();
        phrase = analyzeKeywords(phrase);
        if (phrase.size() == 0) {
            return documentList.iterator();
        } else if (phrase.size() == 1) {
            return searchQuery(phrase.get(0));
        }

        for (int i = 0; i < SegmentNum; i++) {
            List<Integer> currentSegmentList = searchKeywordInSingleSegment(phrase.get(0), i);
            // traverse keywords
            for (int j = 0; j < phrase.size() - 1; j++) {
                List<Integer> secondList = searchKeywordInSingleSegment(phrase.get(j + 1), i);
                currentSegmentList = mergeAndKeywordID(currentSegmentList, secondList);
                if (currentSegmentList.size() == 0) {
                    break;
                }
            }
            // find the positional information about the list
            if (currentSegmentList.size() > 0) {
                Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionary" + i);
                PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);

                Path invertedListFilePath = Paths.get(currentFolder + File.separator + "invertedList" + i);
                PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListFilePath);

                Path positionalListFilePath = Paths.get(currentFolder + File.separator + "positionalList" + i);
                PageFileChannel positionalListPageFileChannel = PageFileChannel.createOrOpen(positionalListFilePath);

                LinkedHashMap<String, KeyInfo> keywordList = new LinkedHashMap<>();
                constructKeywordList(keywordList, dictionaryPageFileChannel, i);

                // we have already know all keywords exists in some documents
                // compare whether there is overlap within a single document
                for (int k = 0; k < phrase.size() - 1; k++) {
                    String first = phrase.get(k);
                    String second = phrase.get(k + 1);

                    KeyInfo firstKeywordKeyInfo = keywordList.get(first);
                    KeyInfo secondKeywordKeyInfo = keywordList.get(second);

                    TreeMap<Integer, PositionalInfo> firstPositionalMap = new TreeMap<>();
                    TreeMap<Integer, PositionalInfo> secondPositionalMap = new TreeMap<>();

                    constructPositionMapForKeyword(invertedListPageFileChannel, firstKeywordKeyInfo, 0, firstPositionalMap);
                    constructPositionMapForKeyword(invertedListPageFileChannel, secondKeywordKeyInfo, 0, secondPositionalMap);

                    // need to avoid concurrent modification error
                    List<Integer> docIDNeededToDelete = new ArrayList<>();

                    for (int docID : currentSegmentList) {
                        PositionalInfo positionalInfo1 = firstPositionalMap.get(docID);
                        PositionalInfo positionalInfo2 = secondPositionalMap.get(docID);

                        byte[] positional1 = readBytesWithCertainOffsetAndLength(positionalListPageFileChannel,
                                positionalInfo1.positionalOffset, positionalInfo1.positionalLength);
                        byte[] positional2 = readBytesWithCertainOffsetAndLength(positionalListPageFileChannel,
                                positionalInfo2.positionalOffset, positionalInfo2.positionalLength);

                        List<Integer> positionsList1 = iCompressor.decode(positional1);
                        List<Integer> positionsList2 = iCompressor.decode(positional2);

                        boolean exist = false;

                        for (int pos : positionsList1) {
                            if (positionsList2.contains(pos + 1)) {
                                exist = true;
                            }
                        }
                        if (!exist) {
                            // this doc ID don't have the second keyword at its next position
                            docIDNeededToDelete.add(docID);
                        }
                    }

                    for (int docID : docIDNeededToDelete) {
                        currentSegmentList.remove(currentSegmentList.indexOf(docID));
                    }
                }
            }
            if (currentSegmentList.size() > 0) {
                String dbFilePath = currentFolder + File.separator + "Mapdb" + i + ".db";
                DocumentStore documentStore = MapdbDocStore.createOrOpen(dbFilePath);
                for (Integer docID : currentSegmentList) {
                    Document currentDocument = documentStore.getDocument(docID);
                    if (currentDocument != null) {
                        documentList.add(currentDocument);
                    }
                }
                documentStore.close();
            }
        }
        return documentList.iterator();
    }

    /**
     * Performs top-K ranked search using TF-IDF.
     * Returns an iterator that returns the top K documents with highest TF-IDF scores.
     *
     * Each element is a pair of <Document, Double (TF-IDF Score)>.
     *
     * If parameter `topK` is null, then returns all the matching documents.
     *
     * Unlike Boolean Query and Phrase Query where order of the documents doesn't matter,
     * for ranked search, order of the document returned by the iterator matters.
     *
     * @param keywords, a list of keywords in the query
     * @param topK, number of top documents weighted by TF-IDF, all documents if topK is null
     * @return a iterator of top-k ordered documents matching the query
     */
    public Iterator<Pair<Document, Double>> searchTfIdf(List<String> keywords, Integer topK) {
        Map<DocID, Double> dotProductAccumulator = new HashMap<>();
        Map<DocID, Double> vectorLengthAccumulator = new HashMap<>();
        int numAllDocs = documentMap.size();
        for (int segmentNum = 0; segmentNum < SegmentNum; segmentNum++) {
            numAllDocs += getNumDocuments(segmentNum);
        }
        Map<String, Double> idfMap = getIdfMap(keywords, numAllDocs);
        List<String> analyzedKeywords = analyzeKeywords(keywords);

        for (int segmentID = 0; segmentID < SegmentNum; segmentID++) {
            List<DocID> docIDInTheSegment = new ArrayList<>();
            Map<String, Double> queryTfIdf = new HashMap<>();
            for (String keyword : analyzedKeywords) {
                int tfQuery = 0;
                for (String kw : analyzedKeywords) {
                    if (kw.equals(keyword)) {
                        tfQuery++;
                    }
                }
                List<Integer> postingList = searchKeywordInSingleSegment(keyword, segmentID);
                double idf = idfMap.get(keyword) == null ? idf(keyword, numAllDocs) : idfMap.get(keyword);
                queryTfIdf.put(keyword, idf * tfQuery);
                List<Integer> tfList = getTfListFromSearch(keyword, segmentID);
                for (int j = 0; j < postingList.size(); j++) {
                    int localDocID = postingList.get(j);
                    DocID docID = new DocID(segmentID, localDocID);
                    int local = j;
                    while (local >= tfList.size()) local--;
                    int tf = tfList.get(local);
                    if (tf == 0) continue;
                    docIDInTheSegment.add(docID);
                    double tfidf = tf * idf;
                    dotProductAccumulator.put(docID,
                            dotProductAccumulator.get(docID) == null ?
                                    tfidf * queryTfIdf.get(keyword) : dotProductAccumulator.get(docID) + tfidf * queryTfIdf.get(keyword));

                    vectorLengthAccumulator.put(docID,
                            vectorLengthAccumulator.get(docID) == null ?
                                    tfidf * tfidf : vectorLengthAccumulator.get(docID) + tfidf * tfidf);
                    double score = dotProductAccumulator.get(docID) / Math.sqrt(vectorLengthAccumulator.get(docID));
                    Iterator<DocID> topKIterator = topKDocList.iterator();
                    while (topKIterator.hasNext()) {
                        DocID doc = topKIterator.next();
                        if (doc.equals(docID)) {
                            topKIterator.remove();
                        }
                    }
                    if (vectorLengthAccumulator.get(docID) == 0) score = 0;
                    docID.tfidf = score;
                    topKDocList.add(docID);
                }
            }

        }

        List<Pair<Document, Double>> resList = new ArrayList<>();
        while (!topKDocList.isEmpty()) {
            DocID doc = topKDocList.poll();
            Document document = getDocumentByID(doc);
            double tfidf = doc.tfidf;
            resList.add(new Pair<>(document, tfidf));
        }
        if (topK == null) return resList.iterator();
        resList = resList.stream().limit(topK).collect(Collectors.toList());
        topKDocList.clear();
        for (int i = 0; i < resList.size() - 1; i++) {
            for (int j = 1; j < resList.size() - i; j++) {
                Pair<Document, Double> a;
                if ((resList.get(j - 1)).getRight().compareTo(resList.get(j).getRight()) < 0) {
                    a = resList.get(j - 1);
                    resList.set((j - 1), resList.get(j));
                    resList.set(j, a);
                }
            }
        }
        return resList.iterator();
    }

    private List<Integer> getTfListFromSearch(String keyword, int segmentID) {
        Path invertedListFilePath = Paths.get(currentFolder + File.separator + "invertedList" + segmentID);
        PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListFilePath);
        Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionary" + segmentID);
        PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);
        LinkedHashMap<String, KeyInfo> keywordList = new LinkedHashMap<>();
        constructKeywordList(keywordList, dictionaryPageFileChannel, segmentID);
        int keywordIndex = -1;
        for (Map.Entry<String, KeyInfo> entry : keywordList.entrySet()) {
            keywordIndex++;
            if (entry.getKey().equals(keyword)) break;
        }

        List<List<Integer>> wholeTfList = new ArrayList<>();

        constructTfList(wholeTfList, keywordList, invertedListPageFileChannel);
        List<Integer> tfListForKeyword = wholeTfList.get(keywordIndex);
        return tfListForKeyword;
    }

    private Document getDocumentByID(DocID docID) {
        String dirPath = currentFolder + File.separator + "Mapdb" + docID.SegmentID + ".db";
        DocumentStore storage = MapdbDocStore.createOrOpen(dirPath);
        Document document = storage.getDocument(docID.LocalDocID);
        storage.close();
        return document;
    }

    private Map<String, Double> getIdfMap(List<String> tokens, int numAllDocs) {
        Map<String, Double> res = new HashMap<>();
        for (String token : tokens) res.put(token, idf(token, numAllDocs));
        return res;
    }

    private double idf(String token, int numAllDocs) {
        double docFreq = 0;

        for (int segmentNum = 0; segmentNum < SegmentNum; segmentNum++) {
            docFreq += getDocumentFrequency(segmentNum, token);
        }
        if (documentMap != null) {
            for (Map.Entry<Integer, Document> entry : documentMap.entrySet()) {
                String text = entry.getValue().getText();
                List<String> words = iAnalyzer.analyze(text);
                for (String word : words) {
                    if (word.equals(token)) {
                        docFreq++;
                        break;
                    }
                }
            }
        }
        return Math.log(numAllDocs / docFreq) / Math.log(10);
    }

    /**
     * Returns the total number of documents within the given segment.
     */
    public int getNumDocuments(int segmentNum) {
        String dirPath = currentFolder + File.separator + "Mapdb" + segmentNum + ".db";
        DocumentStore storage = MapdbDocStore.createOrOpen(dirPath);
        int numDocuments = (int)storage.size();
        storage.close();
        return numDocuments;
    }

    /**
     * Returns the number of documents containing the token within the given segment.
     * The token should be already analyzed by the analyzer. The analyzer shouldn't be applied again.
     */
    public int getDocumentFrequency(int segmentNum, String token) {
        String dirPath = currentFolder + File.separator + "Mapdb" + segmentNum + ".db";
        DocumentStore storage = MapdbDocStore.createOrOpen(dirPath);
        Iterator<Map.Entry<Integer, Document>> documentIterator = storage.iterator();
        int counter = 0;
        while (documentIterator.hasNext()) {
            Map.Entry<Integer, Document> entry = documentIterator.next();
            List<String> wordsInDoc = iAnalyzer.analyze(entry.getValue().getText());
            for (String word : wordsInDoc) {
                if (word.equals(token)) {
                    counter++;
                    break;
                }
            }
        }
        storage.close();
        return counter;
    }

    /**
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        List<Document> documentList = new ArrayList<>();
        for (int i = 0; i < SegmentNum; i++) {
            String dirPath = currentFolder + File.separator + "Mapdb" + i + ".db";
            DocumentStore storage = MapdbDocStore.createOrOpen(dirPath);
            Iterator<Map.Entry<Integer, Document>> storageIterator = storage.iterator();
            while (storageIterator.hasNext()) {
                Map.Entry<Integer, Document> entry = storageIterator.next();
                documentList.add(entry.getValue());
            }
            storage.close();
        }

        return documentList.iterator();
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the total number of segments in the inverted index.
     * This function is used for checking correctness in test cases.
     *
     * @return number of index segments.
     */
    public int getNumSegments() {
        return SegmentNum;
    }

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        if (getNumSegments() == 0) return null;

        LinkedHashMap<String, List<Integer>> invertedLists = new LinkedHashMap<>();
        constructInvertedList(invertedLists, segmentNum);

        LinkedHashMap<Integer, Document> documents = new LinkedHashMap<>();
        constructDocumentMap(documents, segmentNum);

        InvertedIndexSegmentForTest iiSegment = new InvertedIndexSegmentForTest(invertedLists, documents);
        return iiSegment;
    }

    /**
     * Reads a disk segment of a positional index into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public PositionalIndexSegmentForTest getIndexSegmentPositional(int segmentNum) {
        if (getNumSegments() == 0) return null;

        LinkedHashMap<String, List<Integer>> invertedLists = new LinkedHashMap<>();
        Table<String, Integer, List<Integer>> positions = HashBasedTable.create();
        constructInvertedAndPositional(invertedLists, positions, segmentNum);

        LinkedHashMap<Integer, Document> documents = new LinkedHashMap<>();
        constructDocumentMap(documents, segmentNum);

        PositionalIndexSegmentForTest piSegment = new PositionalIndexSegmentForTest(invertedLists, documents, positions);
        return piSegment;
    }

    private void constructInvertedList(LinkedHashMap<String, List<Integer>> invertedLists, int segmentNum) {

        Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionary" + segmentNum);
        PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);

        Path invertedListFilePath = Paths.get(currentFolder + File.separator + "invertedList" + segmentNum);
        PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListFilePath);

        LinkedHashMap<String, KeyInfo> keywordList = new LinkedHashMap<>();
        constructKeywordList(keywordList, dictionaryPageFileChannel, segmentNum);

        for (Map.Entry<String, KeyInfo> entry : keywordList.entrySet()) {
            String keyword = entry.getKey();
            KeyInfo keyInfo = entry.getValue();

            int invertedListOffset = keyInfo.invertedListOffset;
            int invertedDocIDCompressedLen = keyInfo.invertedDocIDCompressedLen;

            byte[] invertedDocIDListBytes = readBytesWithCertainOffsetAndLength(invertedListPageFileChannel, invertedListOffset, invertedDocIDCompressedLen);

            List<Integer> invertedDocIDList = iCompressor.decode(invertedDocIDListBytes);

            invertedLists.put(keyword, invertedDocIDList);
        }
        dictionaryPageFileChannel.close();
        invertedListPageFileChannel.close();
    }

    private void constructInvertedAndPositional(LinkedHashMap<String, List<Integer>> invertedLists, Table<String, Integer, List<Integer>> positions, int segmentNum) {
        Path dictionaryFilePath = Paths.get(currentFolder + File.separator + "dictionary" + segmentNum);
        PageFileChannel dictionaryPageFileChannel = PageFileChannel.createOrOpen(dictionaryFilePath);

        Path invertedListFilePath = Paths.get(currentFolder + File.separator + "invertedList" + segmentNum);
        PageFileChannel invertedListPageFileChannel = PageFileChannel.createOrOpen(invertedListFilePath);

        Path positionalListFilePath = Paths.get(currentFolder + File.separator + "positionalList" + segmentNum);
        PageFileChannel positionListPageFileChannel = PageFileChannel.createOrOpen(positionalListFilePath);

        LinkedHashMap<String, KeyInfo> keywordList = new LinkedHashMap<>();
        constructKeywordList(keywordList, dictionaryPageFileChannel, segmentNum);


        for (Map.Entry<String, KeyInfo> entry : keywordList.entrySet()) {
            String keyword = entry.getKey();
            KeyInfo keyInfo = entry.getValue();

            int invertedListOffset = keyInfo.invertedListOffset;
            int invertedDocIDCompressedLen = keyInfo.invertedDocIDCompressedLen;
            int invertedOffsetCompressedLen = keyInfo.invertedOffsetCompressedLen;

            byte[] invertedDocIDListBytes = readBytesWithCertainOffsetAndLength(invertedListPageFileChannel, invertedListOffset, invertedDocIDCompressedLen);
            int offsetOfOffsetList = invertedListOffset + invertedDocIDCompressedLen;
            byte[] invertedOffsetListBytes = readBytesWithCertainOffsetAndLength(invertedListPageFileChannel, offsetOfOffsetList, invertedOffsetCompressedLen);

            List<Integer> invertedDocIDList = iCompressor.decode(invertedDocIDListBytes);
            List<Integer> invertedOffsetList = iCompressor.decode(invertedOffsetListBytes);

            for (int i = 0; i < invertedDocIDList.size(); i++) {
                int docID = invertedDocIDList.get(i);
                int startOffset = invertedOffsetList.get(i);
                int endOffset = invertedOffsetList.get(i + 1);
                int positionalLength = endOffset - startOffset;
                byte[] positionalListBytes = readBytesWithCertainOffsetAndLength(positionListPageFileChannel, startOffset, positionalLength);
                List<Integer> positionalList = iCompressor.decode(positionalListBytes);
                positions.put(keyword, docID, positionalList);
            }
            invertedLists.put(keyword, invertedDocIDList);
        }
        dictionaryPageFileChannel.close();
        invertedListPageFileChannel.close();
        positionListPageFileChannel.close();
    }

    private void constructDocumentMap(LinkedHashMap<Integer, Document> documents, int segmentNum) {
        String dbFilePath = currentFolder + File.separator + "Mapdb" + segmentNum + ".db";
        DocumentStore mapDB = MapdbDocStore.createOrOpen(dbFilePath);
        Iterator<Map.Entry<Integer, Document>> mapDBIterator = mapDB.iterator();
        while (mapDBIterator.hasNext()) {
            Map.Entry<Integer, Document> entry = mapDBIterator.next();
            documents.put(entry.getKey(), entry.getValue());
        }
        mapDB.close();
    }

    private void writeInvertedListToDiskForInitialSegment(LinkedHashMap<String, List<Integer>> invertedLists, Table<String, Integer, List<Integer>> positions,
                                                          PageFileChannel dictionaryPageFileChannel, PageFileChannel invertedListPageFileChannel, PageFileChannel positionalListPageFileChannel) {
        // fill in the byteBuffer with your data
        ByteBuffer dictionaryByteBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        ByteBuffer invertedListByteBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        ByteBuffer positionalListByteBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);


        // first calculate the number of pages needed to allocate all keyword list
        int counter = 8;
        for (String keyword : invertedLists.keySet()) {
            // 16 bytes reserved for 1. length of the keyword, 2. offset start from the beginning of the invertedListPageFileChannel,
            // 3. compressed docID list length, 4. compressed offset list length
            int currentKeywordTotalLen = 16;
            // we assume the length of the keyword is the number of bytes needed (for english letters),using keyword padding
            currentKeywordTotalLen += ((keyword.length() / 4) + 1) * 4;
            counter += currentKeywordTotalLen;
        }

        int fullPage = counter / PageFileChannel.PAGE_SIZE;
        int pageNum = (fullPage * PageFileChannel.PAGE_SIZE < counter) ? fullPage + 1 : fullPage;

        // first step: put keyword list pageNum into the buffer first
        dictionaryByteBuffer.putInt(pageNum);

        // second step: put the keyword dictionary length into the buffer
        int entrySetSize = invertedLists.entrySet().size();
        dictionaryByteBuffer.putInt(entrySetSize);

        // initialize the cursors in invertedListPageFileChannel
        int invertedListOffset = 0;
        int postionalListOffset = 0;

        // third step: put keywords dictionary into buffer
        for (Map.Entry<String, List<Integer>> entry : invertedLists.entrySet()) {
            int totalLength = 0;
            // insure the sequence in traversing it next is the same
            String keyword = entry.getKey();
            // 3.1 add keyword length, 3.2 add keyword
            addSingleKeywordToDictionary(dictionaryPageFileChannel, dictionaryByteBuffer, keyword);

            // 3.3 add offset in the inverted list
            if (!dictionaryByteBuffer.hasRemaining()) {
                dictionaryPageFileChannel.appendPage(dictionaryByteBuffer);
                dictionaryByteBuffer.clear();
                dictionaryByteBuffer.putInt(invertedListOffset);
            } else {
                dictionaryByteBuffer.putInt(invertedListOffset);
            }

            // 3.4 add length of inverted list and construct inverted list file and positional file
            // three elements construct a cell in inverted list
            // including 1. docID, 2. offset in positional file(postionalListOffset), 3. length of the compressed positional list

            // construct a list ready to be written into the inverted list file
            // For positionMapForCurrentKeyword, key: docID, value: position list
            Map<Integer, List<Integer>> positionMapForCurrentKeyword = positions.row(keyword);
            List<Integer> inverteddocIDListForKeyword = new ArrayList<>();
            List<Integer> invertedOffsetListForKeyword = new ArrayList<>();

            int sizeOfPositonalMap = positionMapForCurrentKeyword.size();
            int positionalCounter = 0;

            for (Map.Entry<Integer, List<Integer>> positionEntry : positionMapForCurrentKeyword.entrySet()) {
                int currentDocID = positionEntry.getKey();
                List<Integer> currentPositionList = positionEntry.getValue();

                // write positional file channel with current positional list
                byte[] currentPositionalArray = iCompressor.encode(currentPositionList);

                int compressedLen = currentPositionalArray.length;

                // write positional list into disk
                writeBytesToDiskWithSpecifiedBuffer(currentPositionalArray, positionalListByteBuffer, positionalListPageFileChannel);

                // add docID, offset in positional file, length of the compressed positional list
                inverteddocIDListForKeyword.add(currentDocID);
                invertedOffsetListForKeyword.add(postionalListOffset);
                positionalCounter++;

                // advance the pointer of positional list
                postionalListOffset += compressedLen;

                // add the last index of positionalListOffset, (exclusive), this is used for finding the length of the last keyword
                if (positionalCounter == sizeOfPositonalMap) {
                    invertedOffsetListForKeyword.add(postionalListOffset);
                }
            }

            // construct the current compressed inverted array
            byte[] currentInvertedDocIDArray = iCompressor.encode(inverteddocIDListForKeyword);
            byte[] currentInvertedOffsetArray = iCompressor.encode(invertedOffsetListForKeyword);

            int invertedListDocIDCompressedLen = currentInvertedDocIDArray.length;
            int invertedListOffsetCompressedLen = currentInvertedOffsetArray.length;
            writeBytesToDiskWithSpecifiedBuffer(currentInvertedDocIDArray, invertedListByteBuffer, invertedListPageFileChannel);
            writeBytesToDiskWithSpecifiedBuffer(currentInvertedOffsetArray, invertedListByteBuffer, invertedListPageFileChannel);
            // add the length for inverted docID list in dictionary
            putIntToSpecifiedBuffer(dictionaryByteBuffer, dictionaryPageFileChannel, invertedListDocIDCompressedLen);

            // add the length for inverted offset list in dictionary
            putIntToSpecifiedBuffer(dictionaryByteBuffer, dictionaryPageFileChannel, invertedListOffsetCompressedLen);

            // advance the pointer for inverted list
            invertedListOffset += (invertedListDocIDCompressedLen + invertedListOffsetCompressedLen);
        }

        // write tf list of each word to the buffer
        int tfSize = 0;
        // store pattern:
        // numKeyword(4 bytes) +
        // for each keyword:
        //      sizeOfDocListForKeyword(4 bytes) + tf of docListForKeyword(size * 4bytes))
        tfSize += 4; // size of invertedlist(keyword list) (4 bytes)
        for (Map.Entry<String, List<Integer>> entry : invertedLists.entrySet()) {
            String keyword = entry.getKey();
            tfSize += 4; // num of docid for this keyword
            tfSize += invertedLists.get(keyword).size() * 4; // tf in each doc
        }
        ByteBuffer tfBuffer = ByteBuffer.allocate(tfSize);

        // populate the tfBuffer
        tfBuffer.putInt(invertedLists.size()); // num of keywords
        for (Map.Entry<String, List<Integer>> entry : invertedLists.entrySet()) {
            String keyword = entry.getKey();
            List<Integer> inverteddocIDListForKeyword = invertedLists.get(keyword);
            tfBuffer.putInt(inverteddocIDListForKeyword.size()); // num of tf
            for (int docID : inverteddocIDListForKeyword) {
                // calc tf there
                Document document = documentMap.get(docID);
                int tfCounter = 0;
                List<String> words = iAnalyzer.analyze(document.getText());
                for (String word : words) {
                    if (word.equals(keyword)) {
                        tfCounter++;
                    }
                }
                tfBuffer.putInt(tfCounter); // tf in each doc
            }
        }
        // write tf list to the channel
        byte[] tfByteArray = tfBuffer.array();
        writeBytesToDiskWithSpecifiedBuffer(tfByteArray, invertedListByteBuffer, invertedListPageFileChannel);

        fillPageEndingWithSpace(dictionaryPageFileChannel, dictionaryByteBuffer);
        fillPageEndingWithSpace(invertedListPageFileChannel, invertedListByteBuffer);
        fillPageEndingWithSpace(positionalListPageFileChannel, positionalListByteBuffer);

        dictionaryPageFileChannel.close();
        invertedListPageFileChannel.close();
        positionalListPageFileChannel.close();
    }

    /**
     *
     * @param segmentInfo
     * @param offset
     * @param dictionaryPageFileChannel
     * @param invertedListPageFileChannel
     * @param positionalListPageFileChannel
     * @return
     */
    private void writeInvertedListToDiskForMergedSegment(int segmentInfo, int offset, PageFileChannel dictionaryPageFileChannel,
                                                         PageFileChannel invertedListPageFileChannel, PageFileChannel positionalListPageFileChannel) {
        int secondSegmentNum = segmentInfo + 1;

        String dictionary1 = currentFolder + File.separator + "dictionary" + segmentInfo;
        String dictionary2 = currentFolder + File.separator + "dictionary" + secondSegmentNum;
        String invertedList1 = currentFolder + File.separator + "invertedList" + segmentInfo;
        String invertedList2 = currentFolder + File.separator + "invertedList" + secondSegmentNum;
        String positionalList1 = currentFolder + File.separator + "positionalList" + segmentInfo;
        String positionalList2 = currentFolder + File.separator + "positionalList" + secondSegmentNum;

        PageFileChannel dictionary1Channel = PageFileChannel.createOrOpen(Paths.get(dictionary1));
        PageFileChannel dictionary2Channel = PageFileChannel.createOrOpen(Paths.get(dictionary2));
        PageFileChannel invertedList1Channel = PageFileChannel.createOrOpen(Paths.get(invertedList1));
        PageFileChannel invertedList2Channel = PageFileChannel.createOrOpen(Paths.get(invertedList2));
        PageFileChannel positionalList1Channel = PageFileChannel.createOrOpen(Paths.get(positionalList1));
        PageFileChannel positionalList2Channel = PageFileChannel.createOrOpen(Paths.get(positionalList2));


        // we assume we can hold all keywords of two segments, but not all posting lists
        // first step: we construct HashMaps for two segment waiting to be merged, HashMap: Key: keyword, Value: KeyInfo object
        LinkedHashMap<String, KeyInfo> keywordList1 = new LinkedHashMap<>();
        LinkedHashMap<String, KeyInfo> keywordList2 = new LinkedHashMap<>();

        constructKeywordList(keywordList1, dictionary1Channel, 1);
        constructKeywordList(keywordList2, dictionary2Channel, 2);

        // second step: construct the merged segment dictionary with keywordList1 and keywordList2
        LinkedHashMap<String, List<KeyInfo>> combinedList = new LinkedHashMap<>();
        for (Map.Entry<String, KeyInfo> entry : keywordList1.entrySet()) {
            String keyword = entry.getKey();
            List<KeyInfo> list = new ArrayList<>();
            list.add(entry.getValue());
            combinedList.put(keyword, list);
        }

        for (Map.Entry<String, KeyInfo> entry : keywordList2.entrySet()) {
            String keyword = entry.getKey();
            if (combinedList.containsKey(keyword)) {
                combinedList.get(keyword).add(entry.getValue());
            } else {
                List<KeyInfo> list = new ArrayList<>();
                list.add(entry.getValue());
                combinedList.put(keyword, list);
            }
        }

        // third step: write combinedList and its corresponding posting lists into disk
        // fill in the byteBuffer with your data
        ByteBuffer dictionaryByteBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        ByteBuffer invertedListByteBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        ByteBuffer positionalListByteBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);

        // first calculate the number of pages needed to allocate all keyword list
        int counter = 8;
        for (Map.Entry<String, List<KeyInfo>> entry : combinedList.entrySet()) {
            String keyword = entry.getKey();
            int currentKeywordTotalLen = 16;
            currentKeywordTotalLen += ((keyword.length() / 4) + 1) * 4;
            counter += currentKeywordTotalLen;
        }

        int fullPage = counter / PageFileChannel.PAGE_SIZE;
        int pageNum = (fullPage * PageFileChannel.PAGE_SIZE < counter) ? fullPage + 1 : fullPage;

        // put keyword list pageNum into the buffer first
        dictionaryByteBuffer.putInt(pageNum);

        // put the keyword dictionary length into the buffer
        int entrySetSize = combinedList.entrySet().size();
        dictionaryByteBuffer.putInt(entrySetSize);

        // cursor in the inverted list file
        int invertedListOffsetCursor = 0;
        int positionalListOffsetCursor = 0;

        // forth step: put keywords dictionary into buffer and write them to disk
        for (Map.Entry<String, List<KeyInfo>> entry : combinedList.entrySet()) {
            String keyword = entry.getKey();

            // step 1,2: add keyword length and keyword to disk
            addSingleKeywordToDictionary(dictionaryPageFileChannel, dictionaryByteBuffer, keyword);

            // step 3: add inverted list offset
            if (!dictionaryByteBuffer.hasRemaining()) {
                dictionaryPageFileChannel.appendPage(dictionaryByteBuffer);
                dictionaryByteBuffer.clear();
                dictionaryByteBuffer.putInt(invertedListOffsetCursor);
            } else {
                dictionaryByteBuffer.putInt(invertedListOffsetCursor);
            }
            // use keyInfoList to construct the new inverted lists
            List<KeyInfo> keyInfoList = entry.getValue();

            // key: docID, value: PositionalInfo
            TreeMap<Integer, PositionalInfo> positionalInfoMapForKeyword = new TreeMap<>();

            // read from dictionary and construct the positional list
            if (keyInfoList.size() == 2) { // keep its origin docID
                KeyInfo keyInfo1 = keyInfoList.get(0);
                constructPositionMapForKeyword(invertedList1Channel, keyInfo1, 0, positionalInfoMapForKeyword);
                KeyInfo keyInfo2 = keyInfoList.get(1);
                constructPositionMapForKeyword(invertedList2Channel, keyInfo2, offset, positionalInfoMapForKeyword);

            } else if (keyInfoList.get(0).segmentNumber == 1) {
                KeyInfo keyInfo1 = keyInfoList.get(0);
                constructPositionMapForKeyword(invertedList1Channel, keyInfo1, 0, positionalInfoMapForKeyword);
            } else if (keyInfoList.get(0).segmentNumber == 2) { // should only have second segment
                KeyInfo keyInfo2 = keyInfoList.get(0);
                constructPositionMapForKeyword(invertedList2Channel, keyInfo2, offset, positionalInfoMapForKeyword);
            }

            // use positionalInfoMapForKeyword to construct new inverted lists for each keyword
            List<Integer> postingDocIDList = new ArrayList<>();
            List<Integer> postingOffsetList = new ArrayList<>();

            int positionalCounter = 0;
            int positionalMapSize = positionalInfoMapForKeyword.size();

            for (Map.Entry<Integer, PositionalInfo> positionalInfoEntry : positionalInfoMapForKeyword.entrySet()) {
                int docID = positionalInfoEntry.getKey();

                postingDocIDList.add(docID);
                postingOffsetList.add(positionalListOffsetCursor);

                // calculate the length of positional list
                PositionalInfo positionalInfo = positionalInfoEntry.getValue();
                int segmentIdentifier = positionalInfo.segmentIdentifier;
                int positionalOffset = positionalInfo.positionalOffset;
                int positionalLength = positionalInfo.positionalLength;

                // write corresponding positional lists to the positional list files
                byte[] result;
                if (segmentIdentifier == 1) {
                    result = readBytesWithCertainOffsetAndLength(positionalList1Channel, positionalOffset, positionalLength);
                } else {
                    result = readBytesWithCertainOffsetAndLength(positionalList2Channel, positionalOffset, positionalLength);
                }

                // append positional lists. this is duplicated, handle it afterwards
                writeBytesToDiskWithSpecifiedBuffer(result, positionalListByteBuffer, positionalListPageFileChannel);

                // advance the positionalListOffsetCursor pointer
                positionalCounter++;
                positionalListOffsetCursor += positionalLength;

                // add the list end offset
                if (positionalCounter == positionalMapSize) {
                    postingOffsetList.add(positionalListOffsetCursor);
                }
            }

            // update the posting list and write it into disk
            byte[] compressedDocIDPostingListBytes = iCompressor.encode(postingDocIDList);
            byte[] compressedOffsetPostingListBytes = iCompressor.encode(postingOffsetList);

            int invertedListDocIDCompressedLen = compressedDocIDPostingListBytes.length;
            int invertedListOffsetCompressedLen = compressedOffsetPostingListBytes.length;

            writeBytesToDiskWithSpecifiedBuffer(compressedDocIDPostingListBytes, invertedListByteBuffer, invertedListPageFileChannel);
            writeBytesToDiskWithSpecifiedBuffer(compressedOffsetPostingListBytes, invertedListByteBuffer, invertedListPageFileChannel);

            // add the length for inverted docID list in dictionary
            putIntToSpecifiedBuffer(dictionaryByteBuffer, dictionaryPageFileChannel, invertedListDocIDCompressedLen);

            // add the length for inverted offset list in dictionary
            putIntToSpecifiedBuffer(dictionaryByteBuffer, dictionaryPageFileChannel, invertedListOffsetCompressedLen);

            // advance the pointer for inverted list
            invertedListOffsetCursor += (invertedListDocIDCompressedLen + invertedListOffsetCompressedLen);
        }

        // fifth step: construct the merged tf list
        List<List<Integer>> tfList1 = new ArrayList<>();
        List<List<Integer>> tfList2 = new ArrayList<>();

        constructTfList(tfList1, keywordList1, invertedList1Channel);
        constructTfList(tfList2, keywordList2, invertedList2Channel);

        List<List<Integer>> mergedTfList = new ArrayList<>();
        mergedTfList.addAll(tfList1);
        mergedTfList.addAll(tfList2);

        // write merged tf for each word to the buffer
        // allocate numKeyword(4 bytes) + foreach keyword:
        //      numTf(4 bytes) + tfListSize * tf(4 bytes)
        //      while num of keyword is equal to the size of mergedtfList
        int numTf = 0;
        for (List<Integer> tfList : mergedTfList) {
            numTf += tfList.size();
        }
        ByteBuffer mergedTfBuffer = ByteBuffer.allocate(4 + 4 * mergedTfList.size() + 4 * numTf);
        int numKeyword = mergedTfList.size();
        mergedTfBuffer.putInt(numKeyword);
        for (List<Integer> tfListForEachKeyword : mergedTfList) {
            mergedTfBuffer.putInt(tfListForEachKeyword.size());
            for (int tf : tfListForEachKeyword) {
                mergedTfBuffer.putInt(tf);
            }
        }
        byte[] tfByteArray = mergedTfBuffer.array();
        writeBytesToDiskWithSpecifiedBuffer(tfByteArray, invertedListByteBuffer, invertedListPageFileChannel);

        fillPageEndingWithSpace(dictionaryPageFileChannel, dictionaryByteBuffer);
        fillPageEndingWithSpace(invertedListPageFileChannel, invertedListByteBuffer);
        fillPageEndingWithSpace(positionalListPageFileChannel, positionalListByteBuffer);

        dictionaryPageFileChannel.close();
        invertedListPageFileChannel.close();
        positionalListPageFileChannel.close();
        dictionary1Channel.close();
        dictionary2Channel.close();
        invertedList1Channel.close();
        invertedList2Channel.close();
        positionalList1Channel.close();
        positionalList2Channel.close();

    }

    /**
     * @param keywordList:     the keywordList waiting to be filled
     * @param pageFileChannel: the pageFileChannel waiting to be read
     * @param segmentNumber:   the index of the segment that is being processed (or just a symbol index)
     */
    private void constructKeywordList(LinkedHashMap<String, KeyInfo> keywordList, PageFileChannel pageFileChannel, int segmentNumber) {

        // We assume the page size is larger than 4, we just want to get the first number of the segment in tempbuffer
        ByteBuffer tempBuffer = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        tempBuffer.put(pageFileChannel.readPage(0));
        tempBuffer.flip();
        int keywordPages = tempBuffer.getInt();
        int numOfKeywords1 = tempBuffer.getInt();
        tempBuffer.clear();

        ByteBuffer buffer = pageFileChannel.readAllPages();
        buffer.flip();
        // just advance the pointer

        if (PageFileChannel.PAGE_SIZE * keywordPages != 0) {
            int lengthOfTheKeywordPages = buffer.getInt();
            int numOfKeywords = buffer.getInt();
            for (int j = 0; j < numOfKeywords; j++) {
                int keyLength = buffer.getInt();
                int keyPaddedLength = ((keyLength / 4) + 1) * 4;
                byte[] keyRawBytes = new byte[keyPaddedLength];
                buffer.get(keyRawBytes, 0, keyPaddedLength);
                byte[] keyBytesNoSpaces = Arrays.copyOfRange(keyRawBytes, 0, keyLength);
                String key = new String(keyBytesNoSpaces);

                // read corresponding posting list page number and number of docIDs
                int invertedListOffset = buffer.getInt();
                int invertedDocIDCompressedLen = buffer.getInt();
                int invertedOffsetCompressedLen = buffer.getInt();

                keywordList.put(key, new KeyInfo(segmentNumber, invertedListOffset, invertedDocIDCompressedLen, invertedOffsetCompressedLen));
            }
        }
    }

    private void constructTfList(List<List<Integer>> tfList, LinkedHashMap<String, KeyInfo> infoMap, PageFileChannel pageFileChannel) {
        ByteBuffer buffer = pageFileChannel.readAllPages();
        buffer.flip();
        /**
         * waste all bytes before idf list
         */

        int wasteLen = 0;
        for (KeyInfo keyInfo : infoMap.values()) {
            int invertedDocIDCompressedLen = keyInfo.invertedDocIDCompressedLen;
            int invertedOffsetCompressedLen = keyInfo.invertedOffsetCompressedLen;
            wasteLen += invertedDocIDCompressedLen;
            wasteLen += invertedOffsetCompressedLen;
        }
        byte[] wastBytes = new byte[wasteLen];
        buffer.get(wastBytes, 0, wasteLen);
        /**
         * wasted all bytes before tf list
         */

        // construct the tflist
        int numKeywords = buffer.getInt();
        for (int i = 0; i < numKeywords; i++) {
            List<Integer> curTfList = new ArrayList<>();
            int numTf = buffer.getInt();
            for (int j = 0; j < numTf; j++) {
                int tf = buffer.getInt();
                curTfList.add(tf);
            }
            tfList.add(curTfList);
        }
    }

    private void fillPageEndingWithSpace(PageFileChannel pageFileChannel, ByteBuffer byteBuffer) {
        byte b = ' ';
        // start a new page for every posting list
        if (byteBuffer.remaining() < PageFileChannel.PAGE_SIZE) {
            while (byteBuffer.remaining() != 0) {
                byteBuffer.put(b);
            }
            pageFileChannel.appendPage(byteBuffer);
            byteBuffer.clear();
        }
    }

    private byte[] readBytesWithCertainOffsetAndLength(PageFileChannel pageFileChannel, int invertedListOffset, int arrayLen) {
        byte[] result;
        int previousPages = invertedListOffset / PageFileChannel.PAGE_SIZE;
        int positionInPage = invertedListOffset % PageFileChannel.PAGE_SIZE;
        int remainingSpace = PageFileChannel.PAGE_SIZE - positionInPage;
        ByteBuffer tempBuffer;
        ByteBuffer resultBuffer = ByteBuffer.allocate(arrayLen);

        // read remaining parts of the page
        tempBuffer = pageFileChannel.readPage(previousPages);
        byte[] fullPage = tempBuffer.array();

        if (remainingSpace >= arrayLen) {
            // if it is less than the remaining space, just copy and return the result
            result = Arrays.copyOfRange(fullPage, positionInPage, positionInPage + arrayLen);
            resultBuffer.put(fullPage, positionInPage, arrayLen);
            return result;
        } else {
            // if it have multiple pages, read it page by page and put it into resultBuffer
            arrayLen -= remainingSpace;
            byte[] firstPageRemaining = Arrays.copyOfRange(fullPage, positionInPage, PageFileChannel.PAGE_SIZE);
            resultBuffer.put(firstPageRemaining);

            int pageNum = previousPages + 1;

            tempBuffer.position(0);

            while (arrayLen > PageFileChannel.PAGE_SIZE) {
                tempBuffer = pageFileChannel.readPage(pageNum++);
                byte[] currentPage = tempBuffer.array();
                resultBuffer.put(currentPage);
                arrayLen -= PageFileChannel.PAGE_SIZE;
                tempBuffer.clear();
            }
            if (arrayLen > 0) {
                tempBuffer = pageFileChannel.readPage(pageNum);
                byte[] lastPage = tempBuffer.array();
                tempBuffer.clear();
                resultBuffer.put(lastPage, 0, arrayLen);
            }

        }
        return resultBuffer.array();
    }

    private void writeBytesToDiskWithSpecifiedBuffer(byte[] input, ByteBuffer byteBuffer, PageFileChannel pageFileChannel) {
        // append positional lists. this is duplicated, handle it afterwards
        int positionalLength = input.length;
        if (!byteBuffer.hasRemaining()) {
            pageFileChannel.appendPage(byteBuffer);
            byteBuffer.clear();
        } else {
            int remainingSpace = byteBuffer.remaining();
            if (positionalLength < remainingSpace) { // all bytes can be allocated to current page
                byteBuffer.put(input);
            } else if (positionalLength == remainingSpace) {
                byteBuffer.put(input);
                pageFileChannel.appendPage(byteBuffer);
                byteBuffer.clear();
            } else {
                int byteCursor = 0;
                byteBuffer.put(input, 0, remainingSpace);
                byteCursor += remainingSpace;
                pageFileChannel.appendPage(byteBuffer);

                byteBuffer.position(0);

                int remainingBytes = positionalLength - remainingSpace;
                while (remainingBytes > PageFileChannel.PAGE_SIZE) {
                    byteBuffer.put(input, byteCursor, PageFileChannel.PAGE_SIZE);
                    pageFileChannel.appendPage(byteBuffer);
                    byteBuffer.clear();
                    byteCursor += PageFileChannel.PAGE_SIZE;
                    remainingBytes -= PageFileChannel.PAGE_SIZE;
                }
                if (remainingBytes < PageFileChannel.PAGE_SIZE) {
                    // the remaining part will be appended in fillPageEndingWithSpace() afterwards
                    byteBuffer.put(input, byteCursor, positionalLength - byteCursor);
                }
            }
        }
    }

    /**
     * @param pageFileChannel
     * @param keyInfo
     * @param docIDOffset
     * @param positionalInfoMap
     * @Function find all positional information for a keyword with keyInfo
     */
    private void constructPositionMapForKeyword(PageFileChannel pageFileChannel, KeyInfo keyInfo, int docIDOffset, TreeMap<Integer, PositionalInfo> positionalInfoMap) {
        int segmentIdentifier = keyInfo.segmentNumber;
        int invertedListOffset = keyInfo.invertedListOffset;
        int invertedDocIDCompressedLen = keyInfo.invertedDocIDCompressedLen;
        int invertedOffsetCompressedLen = keyInfo.invertedOffsetCompressedLen;

        byte[] docIDListBytes = readBytesWithCertainOffsetAndLength(pageFileChannel, invertedListOffset, invertedDocIDCompressedLen);
        int offsetForOffsetList = invertedListOffset + invertedDocIDCompressedLen;
        byte[] offsetListBytes = readBytesWithCertainOffsetAndLength(pageFileChannel, offsetForOffsetList, invertedOffsetCompressedLen);

        List<Integer> decodedDocIDInvertedList = iCompressor.decode(docIDListBytes);
        List<Integer> decodedOffsetInvertedList = iCompressor.decode(offsetListBytes);

        for (int i = 0; i < decodedDocIDInvertedList.size(); i++) {
            int docID = decodedDocIDInvertedList.get(i);
            int startOffset = decodedOffsetInvertedList.get(i);
            int endOffset = decodedOffsetInvertedList.get(i + 1);
            int positionalLength = endOffset - startOffset;
            int newDocID = docID + docIDOffset;
            PositionalInfo positionalInfo = new PositionalInfo(newDocID, segmentIdentifier, startOffset, positionalLength);
            positionalInfoMap.put(newDocID, positionalInfo);
        }
    }

    private void addSingleKeywordToDictionary(PageFileChannel pageFileChannel, ByteBuffer byteBuffer, String keyword) {
        // 3.1 add keyword length
        int keyBytesLength = keyword.getBytes().length;
        if (!byteBuffer.hasRemaining()) {
            pageFileChannel.appendPage(byteBuffer);
            byteBuffer.clear();
            byteBuffer.putInt(keyBytesLength);
        } else {
            byteBuffer.putInt(keyBytesLength);
        }
        // 3.2 add keyword
        byte[] keyBytesTemp = keyword.getBytes();
        int keyPaddedLength = ((keyBytesLength / 4) + 1) * 4;
        byte[] keyBytes = new byte[keyPaddedLength];
        for (int i = 0; i < keyBytesLength; i++) {
            keyBytes[i] = keyBytesTemp[i];
        }
        for (int i = keyBytesLength; i < keyPaddedLength; i++) {
            keyBytes[i] = ' ';
        }
        if (!byteBuffer.hasRemaining()) {
            pageFileChannel.appendPage(byteBuffer);
            byteBuffer.clear();
            byteBuffer.put(keyBytes);
        } else if (byteBuffer.remaining() < keyPaddedLength) {
            byte[] keyBytesArr1 = Arrays.copyOfRange(keyBytes, 0, byteBuffer.remaining());
            byte[] keyBytesArr2 = Arrays.copyOfRange(keyBytes, byteBuffer.remaining(), keyPaddedLength);
            byteBuffer.put(keyBytesArr1);
            pageFileChannel.appendPage(byteBuffer);
            byteBuffer.clear();
            byteBuffer.put(keyBytesArr2);
        } else {
            byteBuffer.put(keyBytes);
        }
    }

    private void putIntToSpecifiedBuffer(ByteBuffer buffer, PageFileChannel pageFileChannel, int number) {
        if (!buffer.hasRemaining()) {
            pageFileChannel.appendPage(buffer);
            buffer.clear();
            buffer.putInt(number);
        } else {
            buffer.putInt(number);
        }
    }
}
