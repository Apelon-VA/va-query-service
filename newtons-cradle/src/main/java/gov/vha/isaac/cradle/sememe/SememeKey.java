package gov.vha.isaac.cradle.sememe;

/**
 * Created by kec on 12/18/14.
 */
public class SememeKey implements Comparable<SememeKey> {
    int key1;
    int key2;
    int sememeSequence;

    public SememeKey(int key1, int key2, int sememeSequence) {
        this.key1 = key1;
        this.key2 = key2;
        this.sememeSequence = sememeSequence;
    }

    @Override
    public int compareTo(SememeKey o) {
        if (key1 != o.key1) {
            if (key1 < o.key1) {
                return -1;
            }
            return 1;
        }
        if (key2 != o.key2) {
            if (key2 < o.key2) {
                return -1;
            }
            return 1;
        }
        if (sememeSequence == o.sememeSequence) {
            return 0;
        }
        if (sememeSequence < o.sememeSequence) {
            return -1;
        }
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SememeKey sememeKey = (SememeKey) o;

        if (key1 != sememeKey.key1) return false;
        if (key2 != sememeKey.key2) return false;
        return sememeSequence == sememeKey.sememeSequence;
    }

    @Override
    public int hashCode() {
        int result = key1;
        result = 31 * result + key2;
        result = 31 * result + sememeSequence;
        return result;
    }

    public int getKey1() {
        return key1;
    }

    public int getKey2() {
        return key2;
    }

    public int getSememeSequence() {
        return sememeSequence;
    }

    @Override
    public String toString() {
        return "SememeKey{" +
                "key1=" + key1 +
                ", key2=" + key2 +
                ", sememeSequence=" + sememeSequence +
                '}';
    }
}
