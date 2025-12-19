package com.example.game;

import java.util.Objects;

public class Player extends Character implements IControllable {
    private final Inventory inventory;
    private Weapon equippedWeapon;
    private Armor equippedArmor;
    private final QuestLog questLog;

    public Player(String id, Position position) {
        super(id, position, 120, 2);
        this.inventory = new Inventory(60);
        this.questLog = new QuestLog();
    }

    public Inventory getInventory() { return inventory; }
    public QuestLog getQuestLog() { return questLog; }

    public Weapon getEquippedWeapon() { return equippedWeapon; }
    public void setEquippedWeapon(Weapon weapon) { this.equippedWeapon = weapon; }

    public Armor getEquippedArmor() { return equippedArmor; }

    public void equipArmor(Armor armor) {
        this.equippedArmor = armor;
        recalcDefense();
    }

    public void unequipArmor() {
        this.equippedArmor = null;
        recalcDefense();
    }

    private void recalcDefense() {
        this.baseDefense = 2 + (equippedArmor != null ? equippedArmor.getDefense() : 0);
    }

    public void attack(Enemy enemy) {
        int dmg = (equippedWeapon != null ? equippedWeapon.getDamage() : 3);
        enemy.takeDamage(dmg);
    }

    public boolean pickup(Item item) {
        return inventory.add(item);
    }

    public void use(Item item) {
        Objects.requireNonNull(item, "item");
        item.use(this);
        if (item instanceof Potion) {
            inventory.remove(item);
        }
    }

    @Override
    public void move(Direction direction) {
        switch (direction) {
            case UP: position.translate(0, -1); break;
            case DOWN: position.translate(0, 1); break;
            case LEFT: position.translate(-1, 0); break;
            case RIGHT: position.translate(1, 0); break;
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public void update(GameWorld world) {
        GameMap map = world.getMap();
        if (map != null) {
            MapTile tile = map.getTile(position);
            if (tile != null) tile.onEnter(this, world);
        }
    }
}

