import java.util.*;

/**
 * User: svasilinets
 * Date: 13.03.12
 * Time: 14:40
 */
class LightMTreeMap<K, V> {

    private TreeMap<K, List<V>> map;

    LightMTreeMap() {
        this.map = new TreeMap<K, List<V>>();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        for (K k: map.keySet()){
            if (map.get(k).contains(value)){
                return true;
            }    
        }
        return false;
    }

    
    public V remove(K key){
        List<V> l = map.get(key);
        if (l == null){
            return null;
        }
        
        V res = l.remove(0);
        if (l.isEmpty()){
            map.remove(key);
        }
        return res;
                
    }
    
    
    public K firstKey(){
        return map.firstKey();
    }
    
    public void put(K key, V value) {
        List<V> l = map.get(key);
        if (l == null){
            l = new LinkedList<V>();
            map.put(key, l);
        }
                     
        l.add(value);
    }

    
    public Set<K> keySet(){
        return map.keySet();
    }
    
    public NavigableSet<K> descendingKeySet(){
        return map.descendingKeySet();
    }

    public void putAll(Map<? extends K, ? extends V> m){
        for (Map.Entry<? extends K, ? extends V> entry: m.entrySet()){
            put(entry.getKey(), entry.getValue());
        }
    }
    
    
    public Iterable<V> getAllByKey(K key){
        List<V> result = map.get(key);
        if (result == null){
            result = new LinkedList<V>();
        }
        return result;
    }

}
