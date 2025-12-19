package com.example.game;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GameWorld {
    private final List<GameObject> objects = new ArrayList<>();
    private GameMap map;

    public void setMap(GameMap map) {
        this.map = map;
    }

    public GameMap getMap() {
        return map;
    }

    public void addObject(GameObject obj) {
        if (obj != null) objects.add(obj);
    }

    public void removeObject(GameObject obj) {
        objects.remove(obj);
    }

    public List<GameObject> getObjects() {
        return Collections.unmodifiableList(objects);
    }

    public Player findPlayer() {
        for (GameObject o : objects) {
            if (o instanceof Player) return (Player) o;
        }
        return null;
    }

    public List<Enemy> findEnemies() {
        List<Enemy> list = new ArrayList<>();
        for (GameObject o : objects) {
            if (o instanceof Enemy) list.add((Enemy) o);
        }
        return list;
    }

    public List<NPC> findNpcs() {
        List<NPC> list = new ArrayList<>();
        for (GameObject o : objects) {
            if (o instanceof NPC) list.add((NPC) o);
        }
        return list;
    }

    public void tick() {
        List<GameObject> snapshot = new ArrayList<>(objects);
        for (GameObject o : snapshot) {
            o.update(this);
        }
        cleanupDeadEnemies();
    }

    private void cleanupDeadEnemies() {
        List<GameObject> toRemove = new ArrayList<>();
        for (GameObject o : objects) {
            if (o instanceof Enemy) {
                Enemy e = (Enemy) o;
                if (!e.isAlive()) toRemove.add(o);
            }
        }
        objects.removeAll(toRemove);
    }
}

