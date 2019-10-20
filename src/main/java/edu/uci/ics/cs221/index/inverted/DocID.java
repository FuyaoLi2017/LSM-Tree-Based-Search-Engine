package edu.uci.ics.cs221.index.inverted;

public class DocID implements Comparable<DocID> {
    int SegmentID;
    int LocalDocID;
    double tfidf;

    public DocID(int SegmentID, int LocalDocID) {
        this.SegmentID = SegmentID;
        this.LocalDocID = LocalDocID;
    }

    /**
     * Rewrite hashcode and equals functions to avoid "duplicate" keys,
     * since we are storing objects (DocIDs) as keys in the maps.
     */

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result + (SegmentID ^ (SegmentID >>> 32));
        result = prime * result + (LocalDocID ^ (LocalDocID >>> 32));

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;

        DocID other = (DocID) obj;
        if (SegmentID != other.SegmentID) return false;
        if (LocalDocID != other.LocalDocID) return false;

        return true;
    }

    @Override
    public int compareTo(DocID b) {
        if(this.tfidf > b.tfidf) return 1;
        if(this.tfidf < b.tfidf) return -1;
        return 0;
    }
}
