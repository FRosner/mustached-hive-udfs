package de.frosner.hive.datastructure;

import java.util.Comparator;

public class OrderedPair<V, O extends Comparable> {

    private final V value;
    private final O orderingValue;

    public OrderedPair(V value, O orderingValue) {
        this.value = value;
        this.orderingValue = orderingValue;
    }

    public V getValue() {
        return value;
    }

    public O getOrderingValue() {
        return orderingValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderedPair)) return false;

        OrderedPair that = (OrderedPair) o;

        if (orderingValue != null ? !orderingValue.equals(that.orderingValue) : that.orderingValue != null)
            return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (orderingValue != null ? orderingValue.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "OrderedPair{" +
                "value=" + value +
                ", orderingValue=" + orderingValue +
                '}';
    }

    public static Comparator<OrderedPair> comparator() {
        return new Comparator<OrderedPair>() {
            @Override
            public int compare(OrderedPair pair1, OrderedPair pair2) {
                return pair1.getOrderingValue().compareTo(pair2.getOrderingValue());
            }
        };
    }

}
