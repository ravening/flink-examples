package com.rakeshv.flink.stream;

import lombok.*;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Tweet {
    private String language;
    private String text;
    private List<String> tags;
}
