package com.example.game;

public class Potion extends Item {
    private final int healAmount;

    public Potion(String name, int weight, int healAmount) {
        super(name, weight);
        this.healAmount = healAmount;
    }

    public int getHealAmount() { return healAmount; }

    @Override
    public void use(Player player) {
        player.heal(healAmount);
    }
}

