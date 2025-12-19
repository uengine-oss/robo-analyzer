package com.example.game;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QuestLog {
    private final List<Quest> quests = new ArrayList<>();

    public void addQuest(Quest quest) {
        if (quest == null) return;
        if (!hasQuest(quest.getId())) quests.add(quest);
    }

    public boolean hasQuest(String questId) {
        for (Quest q : quests) {
            if (q.getId().equals(questId)) return true;
        }
        return false;
    }

    public List<Quest> getQuests() {
        return Collections.unmodifiableList(quests);
    }

    public void updateAll(GameWorld world, Player player) {
        for (Quest q : quests) {
            q.checkProgress(world, player);
        }
    }
}

