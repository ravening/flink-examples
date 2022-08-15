package com.rakeshv.flink.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapToTweets implements MapFunction<String, Tweet> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Tweet map(String s) throws Exception {
        JsonNode tweetJson = mapper.readTree(s);
        JsonNode textNode = tweetJson.get("text");
        JsonNode langNode = tweetJson.get("lang");

        String text = textNode == null ? "" : textNode.textValue();
        String lang = langNode == null ? "en" : langNode.textValue();

        List<String> tags = new ArrayList<>();

        JsonNode entities = tweetJson.get("entities");

        if (entities != null) {
            JsonNode hashTags = entities.get("hashtags");

            for (Iterator<JsonNode> iter = hashTags.elements(); iter.hasNext(); ) {
                JsonNode node = iter.next();
                String hashTag = node.get("text").textValue();
                tags.add(hashTag);
            }
        }

        return Tweet.builder()
                .language(lang)
                .text(text)
                .tags(tags).build();
    }
}
