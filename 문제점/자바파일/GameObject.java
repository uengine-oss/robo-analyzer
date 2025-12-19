package com.example.game;

public abstract class GameObject {
    protected final String id;
    protected final Position position;

    protected GameObject(String id, Position position) {
        this.id = id;
        this.position = new Position(position);
    }

    public String getId() { return id; }
    public Position getPosition() { return position; }

    public abstract void update(GameWorld world);
}

