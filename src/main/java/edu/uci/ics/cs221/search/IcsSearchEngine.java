package edu.uci.ics.cs221.search;

import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class IcsSearchEngine {

    private Path documentDirectory;
    private InvertedIndexManager indexManager;
    private int numOfDocuments = 0;
    private double[] pageRank;

    /**
     * Initializes an IcsSearchEngine from the directory containing the documents and the
     */
    public static IcsSearchEngine createSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        return new IcsSearchEngine(documentDirectory, indexManager);
    }

    private IcsSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        this.documentDirectory = documentDirectory;
        this.indexManager = indexManager;
    }

    /**
     * Writes all ICS web page documents in the document directory to the inverted index.
     */
    public void writeIndex() {
        String documentsPath = documentDirectory + File.separator + "cleaned";
        File cleanedFilePath = new File(documentsPath);
        String[] files = cleanedFilePath.list();
        numOfDocuments = files.length;
        try {
            for (String file : files) {

                StringBuilder document = new StringBuilder();
                String filePath = documentsPath + File.separator + file;
                List<String> allLines = Files.readAllLines(Paths.get(filePath));
                for (int i = 0; i < allLines.size() - 1; i++) {
                    document.append(allLines.get(i)).append('\n');
                }
                document.append(allLines.get(allLines.size() - 1));
                indexManager.addDocument(new Document(document.toString()));

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Computes the page rank score from the "id-graph.tsv" file in the document directory.
     * The results of the computation can be saved in a class variable and will be later retrieved by `getPageRankScores`.
     */
    public void computePageRank(int numIterations) {
        if (numOfDocuments == 0) {
            System.out.println("No document is added.");
        }
        int documentNum = 0;
        try {
            List<String> allLines = Files.readAllLines(Paths.get(documentDirectory + File.separator + "url.tsv"));
            documentNum = allLines.size();
            pageRank = new double[documentNum];
            Arrays.fill(pageRank, 1.0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // construct a map to store the relationship between pages
        // key: docID, value: docIDs pointing to this docID
        Map<Integer, Set<Integer>> idInGraph = new HashMap<>();
        int[] degree = new int[documentNum];
        String idGraphPath = documentDirectory + File.separator + "id-graph.tsv";
        try {
            List<String> allLines = Files.readAllLines(Paths.get(idGraphPath));
            for (String line : allLines) {
                String[] relationship = line.trim().split("\\s");
                int fromId = Integer.parseInt(relationship[0].trim());
                int toId = Integer.parseInt(relationship[1].trim());

                if (idInGraph.containsKey(toId)) {
                    idInGraph.get(toId).add(fromId);
                } else {
                    Set<Integer> set = new HashSet<>();
                    set.add(fromId);
                    idInGraph.put(toId, set);
                }

                degree[fromId]++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < numIterations; i++) {
            double[] temp = new double[documentNum];
            for (int j = 0; j < documentNum; j++) {
                if (idInGraph.containsKey(j)) {
                    Set<Integer> inSet = idInGraph.get(j);
                    double result = 0;
                    for (Integer inDocId : inSet) {
                        double influence = pageRank[inDocId] / degree[inDocId];
                        result += influence;
                    }
                    temp[j] = 0.15 + 0.85 * result;
                } else {
                    temp[j] = 0.15;
                }
            }
            pageRank = temp;
        }
    }


    /**
     * Gets the page rank score of all documents previously computed. Must be called after `computePageRank`.
     * Returns an list of <DocumentID - Score> Pairs that is sorted by score in descending order (high scores first).
     */
    public List<Pair<Integer, Double>> getPageRankScores() {
        List<Pair<Integer, Double>> pageRankList = new ArrayList<>();
        for (int i = 0; i < pageRank.length; i++) {
            pageRankList.add(new Pair<>(i, pageRank[i]));
        }

        Collections.sort(pageRankList, (o1, o2) -> {
            double right1 = o1.getRight();
            double right2 = o2.getRight();
            if (right1 > right2) return -1;
            else if (right1 < right2) return 1;
            return 0;
        });

        return pageRankList;
    }

    /**
     * Searches the ICS document corpus and returns the top K documents ranked by combining TF-IDF and PageRank.
     * <p>
     * The search process should first retrieve ALL the top documents from the InvertedIndex by TF-IDF rank,
     * by calling `searchTfIdf(query, null)`.
     * <p>
     * Then the corresponding PageRank score of each document should be retrieved. (`computePageRank` will be called beforehand)
     * For each document, the combined score is  tfIdfScore + pageRankWeight * pageRankScore.
     * <p>
     * Finally, the top K documents of the combined score are returned. Each element is a pair of <Document, combinedScore>
     * <p>
     * <p>
     * Note: We could get the Document ID by reading the first line of the document.
     * This is a workaround because our project doesn't support multiple fields. We cannot keep the documentID in a separate column.
     */
    public Iterator<Pair<Document, Double>> searchQuery(List<String> query, int topK, double pageRankWeight) {
        Iterator<Pair<Document, Double>> tfIdfIter = indexManager.searchTfIdf(query, null);
        Map<Integer, Double> pageRankMap = new HashMap<>();
        for (int i = 0; i < pageRank.length; i++) {
            pageRankMap.put(i, pageRank[i]);
        }

        // create a min heap based on score, we maintain the pq to have only k elements
        PriorityQueue<Pair<Document, Double>> pq = new PriorityQueue<>((o1, o2) -> {
            if (o1.getRight() > o2.getRight()) return 1;
            if (o1.getRight() < o2.getRight()) return -1;
            return 0;
        });

        while (tfIdfIter.hasNext()) {
            Pair<Document, Double> currentPair = tfIdfIter.next();
            Document document = currentPair.getLeft();
            double tfIdfScore = currentPair.getRight();
            int docID = Integer.valueOf((document.getText().split("\\s"))[0].trim());
            double pageRankScore = pageRankMap.get(docID);
            double finalScore = tfIdfScore + pageRankWeight * pageRankScore;
            if (pq.size() < topK) {
                pq.offer(new Pair<>(document, finalScore));
            } else {
                Pair<Document, Double> pair = pq.peek();
                double currentMinimumScore = pair.getRight();
                if (currentMinimumScore < finalScore) {
                    pq.poll();
                    pq.offer(new Pair<>(document, finalScore));
                }
            }
        }

        // get a reversed order result, then reverse it
        List<Pair<Document, Double>> result = new ArrayList<>();
        while (!pq.isEmpty()) {
            result.add(pq.poll());
        }
        Collections.reverse(result);
        return result.iterator();
    }
}
