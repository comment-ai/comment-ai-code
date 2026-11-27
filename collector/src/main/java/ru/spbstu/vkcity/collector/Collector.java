package ru.spbstu.vkcity.collector;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


class Collector {
    private static final Logger LOG = LoggerFactory.getLogger(Collector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId ZONE = ZoneId.of("Europe/Moscow");
    private static final int PAGE = 100;
    /** Сколько ответов VK отдаёт вместе с комментарием верхнего уровня */
    private static final int THREAD_PREVIEW = 10;

    private final VkApiClient client;
    private final Sink sink;
    private final Integer maxPosts;
    private final boolean withComments;

    Collector(VkApiClient client, Sink sink, Integer maxPosts, boolean withComments) {
        this.client = client;
        this.sink = sink;
        this.maxPosts = maxPosts;
        this.withComments = withComments;
    }

    void run(List<String> groups, LocalDate since, LocalDate until) throws IOException {
        LOG.info("Период: {} — {}, пабликов: {}", since == null ? "начало" : since, until == null ? "сегодня" : until, groups.size());
        for (String g : groups) {
            ObjectNode group;
            try {
                group = fetchGroup(g.trim());
            } catch (VkApiClient.VkApiException e) {
                LOG.warn("Пропускаем '{}': {}", g, e.getMessage());
                continue;
            }
            long groupId = group.path("id").asLong();
            String screenName = group.path("screen_name").asText("club" + groupId);
            group.put("_collected_at", Instant.now().toString());
            sink.group(group);

            if (group.path("is_closed").asInt(0) != 0) {
                LOG.warn("Паблик '{}' закрытый, стену прочитать нельзя", g);
                continue;
            }
            collectGroup(groupId, screenName, since, until);
        }
    }

    private ObjectNode fetchGroup(String raw) throws VkApiClient.VkApiException {
        String id = raw.replaceFirst("^https?://", "").replaceFirst("^(m\\.)?vk\\.(com|ru)/", "").replaceFirst("^@", "");
        JsonNode resp = client.call("groups.getById", Map.of("group_id", id, "fields", "members_count,description,city"));
        JsonNode list = resp.has("groups") ? resp.get("groups") : resp;
        if (!list.isArray() || list.isEmpty()) throw new VkApiClient.VkApiException(-1, "паблик не найден");
        return (ObjectNode) list.get(0);
    }

    private void collectGroup(long groupId, String name, LocalDate since, LocalDate until) throws IOException {
        long ownerId = -groupId;
        int posts = 0, comments = 0, offset = 0;
        outer:
        while (true) {
            JsonNode items = call("wall.get", Map.of("owner_id", ownerId, "count", PAGE, "offset", offset)).path("items");
            if (!items.isArray() || items.isEmpty()) break;

            for (JsonNode node : items) {
                ObjectNode post = (ObjectNode) node;
                LocalDate day = Instant.ofEpochSecond(post.path("date").asLong()).atZone(ZONE).toLocalDate();
                boolean pinned = post.path("is_pinned").asInt(0) == 1;
                // Закреплённый пост может быть старым — по нему нельзя останавливать обход
                if (since != null && day.isBefore(since)) {
                    if (pinned) continue;
                    break outer;
                }
                if (until != null && day.isAfter(until)) continue;

                stamp(post, groupId, name);
                sink.post(post);
                posts++;
                if (withComments && post.path("comments").path("count").asInt(0) > 0) {
                    comments += collectComments(groupId, name, post.path("id").asLong());
                }
                if (maxPosts != null && posts >= maxPosts) break outer;
            }
            if (items.size() < PAGE) break;
            offset += PAGE;
            LOG.info("{}: постов {}, комментариев {}", name, posts, comments);
        }
        LOG.info("{}: итого постов {}, комментариев {}", name, posts, comments);
    }

    private int collectComments(long groupId, String name, long postId) throws IOException {
        long ownerId = -groupId;
        int n = 0, offset = 0;
        while (true) {
            JsonNode items = call("wall.getComments", Map.of(
                    "owner_id", ownerId, "post_id", postId,
                    "count", PAGE, "offset", offset, "sort", "asc",
                    "need_likes", 1, "thread_items_count", THREAD_PREVIEW)).path("items");
            if (!items.isArray() || items.isEmpty()) break;

            for (JsonNode node : items) {
                ObjectNode c = (ObjectNode) node;
                JsonNode thread = c.path("thread");
                if (thread.isObject() && thread.path("count").asInt(0) > thread.path("items").size()) {
                    ((ObjectNode) thread).set("items", fetchThread(ownerId, postId, c.path("id").asLong()));
                }
                stamp(c, groupId, name);
                sink.comment(c);
                n += 1 + c.path("thread").path("items").size();
            }
            if (items.size() < PAGE) break;
            offset += PAGE;
        }
        return n;
    }
}
