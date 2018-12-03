/*
 * CoarseGrainHashMap.java
 *
 * (C) Hans Vandierendonck 2017
 */
import java.util.HashMap;

class CoarseGrainHashMap<K,V> implements Map<K,V> {
    private final HashMap<K,V> map;

    CoarseGrainHashMap( int capacity ) {
	    map = new HashMap<>(capacity);
    }

    synchronized public boolean add(K k, V v) {
        map.put(k, v);
        return map.containsKey(k);
    }
    
    synchronized public boolean remove(K k) {
	    map.remove(k);
	    return !map.containsKey(k);
    }
    
    synchronized public boolean contains(K k) {
        return map.containsKey(k);
    }
    
    synchronized public V get(K k) {
        return map.get(k);
    }

    public int debuggingCountElements() {
        return map.size();
    }
}
