package com.example.game;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Inventory {
    private final int capacity;
    private final List<Item> items = new ArrayList<>();

    public Inventory(int capacity) {
        this.capacity = capacity;
    }

    public int getCapacity() { return capacity; }

    public List<Item> getItems() {
        return Collections.unmodifiableList(items);
    }

    public int totalWeight() {
        int sum = 0;
        for (Item i : items) sum += i.getWeight();
        return sum;
    }

    public boolean add(Item item) {
        if (totalWeight() + item.getWeight() > capacity) return false;
        return items.add(item);
    }

    public boolean remove(Item item) {
        return items.remove(item);
    }

    public Item findByName(String name) {
        for (Item i : items) {
            if (i.getName().equalsIgnoreCase(name)) return i;
        }
        return null;
    }
}

