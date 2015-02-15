package gov.vha.isaac.cradle;

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
public class SememeCollection implements Collection<RefexMember<?, ?>> {

    NavigableSet<SememeKey> keys;
    ConcurrentSequenceSerializedObjectMap<RefexMember<?, ?>> sememeMap;


    public SememeCollection(NavigableSet<SememeKey> keys,
                            ConcurrentSequenceSerializedObjectMap<RefexMember<?, ?>> sememeMap) {
        this.keys = keys;
        this.sememeMap = sememeMap;
    }


    private class SememeIterator implements Iterator<RefexMember<?, ?>> {

        Iterator<SememeKey> sememeNidIterator = keys.iterator();

        @Override
        public boolean hasNext() {
            return sememeNidIterator.hasNext();
        }

        @Override
        public RefexMember<?, ?> next() {
            return (RefexMember<?, ?>)
                    Ts.get().getSememe(sememeNidIterator.next().getSememeSequence());
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
        return new SememeIterator();
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
        return keys.stream().map((sememeKey) -> {return sememeMap.get(sememeKey.sememeSequence).get();});
    }

    @Override
    public Stream<RefexMember<?, ?>> parallelStream() {
        return keys.parallelStream().map((sememeKey) -> {return sememeMap.get(sememeKey.sememeSequence).get();});
    }
}
