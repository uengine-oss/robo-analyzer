package com.example.game;

import java.util.ArrayList;
import java.util.List;

public class Enemy extends Character {
    private final List<Item> drops = new ArrayList<>();

    public Enemy(String id, Position position, int maxHp, int baseDefense) {
        super(id, position, maxHp, baseDefense);
    }

    public void addDrop(Item item) {
        if (item != null) drops.add(item);
    }

    public List<Item> getDrops() {
        return new ArrayList<>(drops);
    }

    public void attack(Player player) {
        player.takeDamage(6);
    }

    @Override
    public void update(GameWorld world) {
        Player p = world.findPlayer();
        if (p != null && isAlive()) {
            int dx = Integer.compare(p.getPosition().getX(), position.getX());
            int dy = Integer.compare(p.getPosition().getY(), position.getY());
            position.translate(dx, dy);
        }
    }
}

