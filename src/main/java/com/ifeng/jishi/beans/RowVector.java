package com.ifeng.jishi.beans;

import java.io.Serializable;
import java.util.List;

public class RowVector  implements Serializable {
    private String id;
    private String  title;
    private List words;

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getWords() {
        return words;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setWords(List words) {
        this.words = words;
    }
}
