package com.example.game;

public class Weapon extends Item {
    private final int damage;

    public Weapon(String name, int weight, int damage) {
        super(name, weight);
        this.damage = damage;
    }

    public int getDamage() { return damage; }

    @Override
    public void use(Player player) {
        player.setEquippedWeapon(this);
    }
}

