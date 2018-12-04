/*
 * SegmentedHashMap.java
 *
 * (C) Hans Vandierendonck, 2017
 */

import java.util.HashMap;
import java.util.Random;

class SegmentedHashMap<K,V> implements Map<K,V> {
    private final HashMap<K,V>[] segments;
    private final int num_segments;

    SegmentedHashMap( int numseg, int capacity ) {
	num_segments = numseg;
	segments = new HashMap[capacity];

	for(int i=0; i<segments.length; i++){
	    segments[i] = new HashMap<>();
	}
    }

    // Select a segment by hashing the key to a value in the range
    // 0 ... num_segments-1. Base yourself on k.hashCode().
    private int hash( K k ) {
        return k.hashCode() & (num_segments - 1);
    }

    public boolean add(K k, V v) {
	    int segKey = hash(k);
	    synchronized(segments[segKey]){
	        segments[segKey].put(k,v);
	        return segments[segKey].containsKey(k);
        }
    }
    
    public boolean remove(K k) {
        int segKey = hash(k);
        synchronized(segments[segKey]){
            segments[segKey].remove(k);
            return !segments[segKey].containsKey(k);
        }
    }
    
    public boolean contains(K k) {
        int segKey = hash(k);
        synchronized (segments[segKey]){
            return segments[segKey].containsKey(k);
        }
    }
    
    public V get(K k) {
        int segKey = hash(k);
        synchronized (segments[segKey]){
            return segments[segKey].get(k);
        }
    }

    public int debuggingCountElements() {
        int count = 0;
        for(int i=0; i<segments.length; i++){
            count += segments[i].size();
        }
        return count;
    }
}
