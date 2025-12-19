package com.example.game;

import java.util.Objects;

public class Position {
    private int x;
    private int y;

    public Position(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public Position(Position other) {
        this.x = other.x;
        this.y = other.y;
    }

    public int getX() { return x; }
    public int getY() { return y; }

    public void translate(int dx, int dy) {
        this.x += dx;
        this.y += dy;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Position)) return false;
        Position p = (Position) o;
        return x == p.x && y == p.y;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}

