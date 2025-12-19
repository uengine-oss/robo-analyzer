package com.example.game;

public class Armor extends Item {
    private final int defense;

    public Armor(String name, int weight, int defense) {
        super(name, weight);
        this.defense = defense;
    }

    public int getDefense() { return defense; }

    @Override
    public void use(Player player) {
        player.equipArmor(this);
    }
}

