package com.example.game;

public abstract class Item {
    protected final String name;
    protected final int weight;

    protected Item(String name, int weight) {
        this.name = name;
        this.weight = weight;
    }

    public String getName() { return name; }
    public int getWeight() { return weight; }

    public abstract void use(Player player);
}

