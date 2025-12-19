package com.example.game;

public abstract class Character extends GameObject implements Damageable {
    protected int maxHp;
    protected int hp;
    protected int baseDefense;

    protected Character(String id, Position position, int maxHp, int baseDefense) {
        super(id, position);
        this.maxHp = maxHp;
        this.hp = maxHp;
        this.baseDefense = baseDefense;
    }

    public int getHp() { return hp; }
    public int getMaxHp() { return maxHp; }
    public int getBaseDefense() { return baseDefense; }

    @Override
    public void takeDamage(int amount) {
        int dmg = Math.max(0, amount - baseDefense);
        hp = Math.max(0, hp - dmg);
    }

    public void heal(int amount) {
        hp = Math.min(maxHp, hp + Math.max(0, amount));
    }

    @Override
    public boolean isAlive() {
        return hp > 0;
    }
}

