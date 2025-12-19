package com.example.game;

public class GameMap {
    private final int width;
    private final int height;
    private final MapTile[][] tiles;

    public GameMap(int width, int height, MapTile defaultTile) {
        this.width = width;
        this.height = height;
        this.tiles = new MapTile[height][width];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                tiles[y][x] = defaultTile;
            }
        }
    }

    public int getWidth() { return width; }
    public int getHeight() { return height; }

    public void setTile(int x, int y, MapTile tile) {
        if (!inBounds(x, y)) return;
        tiles[y][x] = tile;
    }

    public MapTile getTile(Position pos) {
        if (pos == null) return null;
        return getTile(pos.getX(), pos.getY());
    }

    public MapTile getTile(int x, int y) {
        if (!inBounds(x, y)) return null;
        return tiles[y][x];
    }

    public boolean isWalkable(Position pos) {
        MapTile t = getTile(pos);
        return t != null && t.isWalkable();
    }

    private boolean inBounds(int x, int y) {
        return x >= 0 && y >= 0 && x < width && y < height;
    }
}

