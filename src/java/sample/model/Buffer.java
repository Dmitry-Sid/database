package sample.model;

import sample.model.pojo.TableType;

import java.util.Objects;
import java.util.function.Consumer;

public interface Buffer<V extends TableType> {

    public void add(V value, State state);

    public Element<V> get(int id);

    public int size();

    public void stream(Consumer<Element<V>> consumer);

    public void flush();

    public class Element<V> {
        private final V value;
        private final State state;

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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Element)) {
                return false;
            }
            final Element<?> element = (Element<?>) o;
            return Objects.equals(getValue(), element.getValue()) &&
                    getState() == element.getState();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getValue(), getState());
        }
    }

    public enum State {
        ADDED, UPDATED, DELETED
    }

}
