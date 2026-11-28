package ru.spbstu.vkcity.collector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "vk-collector", mixinStandardHelpOptions = true,
        description = "Собирает посты и комментарии из пабликов VK и отправляет их в Kafka и/или в JSONL-файлы")
public class Main implements Callable<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    enum SinkType { kafka, file, both }

    @Option(names = "--token", description = "Ключ доступа VK (по умолчанию VK_TOKEN из окружения или .env)")
    String token;

    @Option(names = "--groups", split = ",", description = "Паблики через запятую: screen_name, ссылка, club123 или id")
    List<String> groups = new ArrayList<>();

    @Option(names = "--groups-file", description = "CSV со списком пабликов (первая колонка — screen_name), например config/sources.csv")
    Path groupsFile;

    @Option(names = "--since", description = "Посты не раньше даты, yyyy-MM-dd")
    LocalDate since;

    @Option(names = "--until", description = "Посты не позже даты, yyyy-MM-dd")
    LocalDate until;

    @Option(names = "--max-posts", description = "Максимум постов на паблик")
    Integer maxPosts;

    @Option(names = "--no-comments", description = "Не собирать комментарии")
    boolean noComments;

    @Option(names = "--rps", defaultValue = "${env:VK_RPS:-3}", description = "Запросов к VK API в секунду (по умолчанию: ${DEFAULT-VALUE})")
    double rps;

    @Option(names = "--sink", defaultValue = "${env:COLLECTOR_SINK:-file}", description = "kafka | file | both (по умолчанию: ${DEFAULT-VALUE})")
    SinkType sinkType;

    @Option(names = "--out-dir", defaultValue = "data", description = "Папка для JSONL (по умолчанию: ${DEFAULT-VALUE})")
    Path outDir;

    @Option(names = "--kafka-bootstrap", defaultValue = "${env:KAFKA_BOOTSTRAP_SERVERS:-localhost:9094}")
    String kafkaBootstrap;

    @Option(names = "--topic-prefix", defaultValue = "${env:KAFKA_TOPIC_PREFIX:-vk.raw.}")
    String topicPrefix;

    @Option(names = "--every-minutes", defaultValue = "0",
            description = "Режим наблюдения: повторять сбор каждые N минут (0 — один проход)")
    int everyMinutes;

    @Option(names = "--lookback-days", defaultValue = "3",
            description = "В режиме наблюдения пересобирать посты за последние N дней, чтобы обновить лайки и комментарии")
    int lookbackDays;



    public static void main(String[] args) {
        CommandLine cli = new CommandLine(new Main());
        cli.registerConverter(LocalDate.class, LocalDate::parse);
        cli.setCaseInsensitiveEnumValuesAllowed(true);
        System.exit(cli.execute(args));
    }
}
