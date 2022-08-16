package com.rakeshv.flink.stream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Tweet {
    private String language;
    private String text;
    private String userName;
    private List<String> tags;
}
