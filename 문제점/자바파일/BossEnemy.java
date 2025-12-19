package com.example.game;

public class BossEnemy extends Enemy {
    private int rage;

    public BossEnemy(String id, Position position) {
        super(id, position, 260, 4);
        this.rage = 0;
    }

    @Override
    public void takeDamage(int amount) {
        super.takeDamage(amount);
        rage = Math.min(100, rage + 10);
    }

    @Override
    public void attack(Player player) {
        int bonus = rage >= 50 ? 8 : 3;
        player.takeDamage(10 + bonus);
        rage = Math.max(0, rage - 20);
    }
}

