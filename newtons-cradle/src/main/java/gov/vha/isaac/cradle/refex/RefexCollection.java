package gov.vha.isaac.cradle.refex;

import gov.vha.isaac.cradle.collections.ConcurrentSequenceSerializedObjectMap;
import org.ihtsdo.otf.tcc.api.store.Ts;
import org.ihtsdo.otf.tcc.model.cc.refex.RefexMember;

import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by kec on 12/18/14.
 */
public class RefexCollection implements Collection<RefexMember<?, ?>> {

    NavigableSet<RefexKey> keys;
    ConcurrentSequenceSerializedObjectMap<RefexMember<?, ?>> refexMap;


    public RefexCollection(NavigableSet<RefexKey> keys,
                            ConcurrentSequenceSerializedObjectMap<RefexMember<?, ?>> refexMap) {
        this.keys = keys;
        this.refexMap = refexMap;
    }


    private class RefexIterator implements Iterator<RefexMember<?, ?>> {

        Iterator<RefexKey> sememeNidIterator = keys.iterator();

        @Override
        public boolean hasNext() {
            return sememeNidIterator.hasNext();
        }

        @Override
        public RefexMember<?, ?> next() {
            return (RefexMember<?, ?>)
                    Ts.get().getRefex(sememeNidIterator.next().getRefexSequence());
        }
    }

    @Override

    public int size() {
        return keys.size();
    }

    @Override
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Iterator<RefexMember<?, ?>> iterator() {
        return new RefexIterator();
    }

    @Override
    public void forEach(Consumer<? super RefexMember<?, ?>> action) {
        iterator().forEachRemaining(action);
    }


    @Override
    public Object[] toArray() {
        return new Object[0];
    }


    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(RefexMember<?, ?> refexMember) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends RefexMember<?, ?>> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super RefexMember<?, ?>> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<RefexMember<?, ?>> stream() {
        return keys.stream().map((refexKey) -> {return refexMap.get(refexKey.refexSequence).get();});
    }

    @Override
    public Stream<RefexMember<?, ?>> parallelStream() {
        return keys.parallelStream().map((refexKey) -> {return refexMap.get(refexKey.refexSequence).get();});
    }
}
