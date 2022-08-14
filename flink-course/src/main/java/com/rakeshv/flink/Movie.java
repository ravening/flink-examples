package com.rakeshv.flink;

import java.util.Set;

public class Movie {
    private int id;
    private String name;
    private Set<String> genres;

    public Movie(int id, String name, Set<String> genres) {
        this.id = id;
        this.name = name;
        this.genres = genres;
    }

    public String getName() {
        return name;
    }

    public Set<String> getGenres() {
        return genres;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "id='" + id + '\'' +
                "name='" + name + '\'' +
                ", genres=" + genres +
                '}';
    }
}
