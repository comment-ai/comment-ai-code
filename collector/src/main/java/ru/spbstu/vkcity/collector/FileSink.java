package ru.spbstu.vkcity.collector;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** пишет в хранилище дарные*/
class FileSink implements Sink {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    final Path runDir;
    private final BufferedWriter groups;
    private final BufferedWriter posts;
    private final BufferedWriter comments;

    FileSink(Path outDir) throws IOException {
        this.runDir = outDir.resolve("run_" + DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now()));
        Files.createDirectories(runDir);
        this.groups = open("groups.jsonl");
        this.posts = open("posts.jsonl");
        this.comments = open("comments.jsonl");
    }

    private BufferedWriter open(String name) throws IOException {
        return Files.newBufferedWriter(runDir.resolve(name), StandardCharsets.UTF_8);
    }

    private static void write(BufferedWriter w, JsonNode node) throws IOException {
        w.write(MAPPER.writeValueAsString(node));
        w.write('\n');
    }

    @Override public void group(JsonNode g) throws IOException { write(groups, g); }
    @Override public void post(JsonNode p) throws IOException { write(posts, p); }
    @Override public void comment(JsonNode c) throws IOException { write(comments, c); }

    @Override
    public void close() throws IOException {
        groups.close();
        posts.close();
        comments.close();
    }
}
