package server.model;

import server.model.pojo.TableType;

import java.util.Objects;

public interface Buffer<V extends TableType> {

    void add(V value, State state);

    Element<V> get(int id);

    int size();

    StoppableStream<Element<V>> stream();

    void flush();

    enum State {
        ADDED, UPDATED, DELETED, READ
    }

    class Element<V> {
        private final V value;
        private final State state;
        private volatile boolean flushed;

        public Element(V value, State state) {
            this.value = value;
            this.state = state;
        }

        public V getValue() {
            return value;
        }

        public State getState() {
            return state;
        }

        public boolean isFlushed() {
            return flushed || State.READ == state;
        }

        public void setFlushed(boolean flushed) {
            this.flushed = flushed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Element)) {
                return false;
            }
            final Element<V> element = (Element<V>) o;
            return isFlushed() == element.isFlushed() &&
                    Objects.equals(getValue(), element.getValue()) &&
                    getState() == element.getState();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getValue(), getState(), isFlushed());
        }
    }

}
