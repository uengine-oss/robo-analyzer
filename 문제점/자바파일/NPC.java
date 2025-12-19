package com.example.game;

public class NPC extends GameObject implements Interactable {
    private final String name;
    private Quest offeredQuest;

    public NPC(String id, Position position, String name) {
        super(id, position);
        this.name = name;
    }

    public String getName() { return name; }

    public void setOfferedQuest(Quest quest) {
        this.offeredQuest = quest;
    }

    public Quest getOfferedQuest() { return offeredQuest; }

    @Override
    public void interact(Player player) {
        if (offeredQuest != null && !player.getQuestLog().hasQuest(offeredQuest.getId())) {
            player.getQuestLog().addQuest(offeredQuest);
        }
    }

    @Override
    public void update(GameWorld world) {
    }
}

