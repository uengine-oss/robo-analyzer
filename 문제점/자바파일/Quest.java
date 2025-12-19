package com.example.game;

import java.util.Objects;

public class Quest {
    private final String id;
    private final String title;
    private final QuestObjective objective;
    private boolean completed;

    public Quest(String id, String title, QuestObjective objective) {
        this.id = Objects.requireNonNull(id);
        this.title = Objects.requireNonNull(title);
        this.objective = Objects.requireNonNull(objective);
        this.completed = false;
    }

    public String getId() { return id; }
    public String getTitle() { return title; }
    public QuestObjective getObjective() { return objective; }
    public boolean isCompleted() { return completed; }

    public void checkProgress(GameWorld world, Player player) {
        if (!completed && objective.isSatisfied(world, player)) {
            completed = true;
        }
    }
}

