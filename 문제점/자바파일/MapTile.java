package com.example.game;

public class MapTile {
    private final boolean walkable;
    private final Interactable interactable;

    public MapTile(boolean walkable, Interactable interactable) {
        this.walkable = walkable;
        this.interactable = interactable;
    }

    public boolean isWalkable() {
        return walkable;
    }

    public Interactable getInteractable() {
        return interactable;
    }

    public void onEnter(Player player, GameWorld world) {
        if (interactable != null) {
            interactable.interact(player);
        }
        player.getQuestLog().updateAll(world, player);
    }
}

