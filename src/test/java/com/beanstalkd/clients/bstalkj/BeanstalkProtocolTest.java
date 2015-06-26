package com.beanstalkd.clients.bstalkj;

import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.BURY;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.DELETE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.IGNORE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.RELEASE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.RESERVE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.RESERVE_WITH_TIMEOUT;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.STATS_TUBE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.USE;
import static com.beanstalkd.clients.bstalkj.BeanstalkProtocol.Command.WATCH;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.regex.Pattern;
import org.junit.Test;

public class BeanstalkProtocolTest {

    @Test
    public void should_create_use_command() throws Exception {
        final String response = "USING random_tube_name";

        assertThat(USE.validResponsePattern.toString()).isEqualTo(Pattern.compile("^USING.*").toString());
        USE.check(response);
    }

    @Test
    public void should_create_watch_command() throws Exception {
        String command = WATCH.get("random_tube_name");

        assertThat(command).isEqualTo("watch random_tube_name\r\n");
        WATCH.check("WATCHING random_tube_name");
    }

    @Test
    public void should_create_ignore_command() throws Exception {
        String command = IGNORE.get("random_tube_name");

        assertThat(command).isEqualTo("ignore random_tube_name\r\n");
        IGNORE.check("WATCHING default");
    }

    @Test
    public void should_create_reserve_command() throws Exception {
        String command = RESERVE.get();

        assertThat(command).isEqualTo("reserve\r\n");
    }

    @Test
    public void should_create_reserve_with_timeout_command() throws Exception {
        String command = RESERVE_WITH_TIMEOUT.get(101);

        assertThat(command).isEqualTo("reserve-with-timeout 101\r\n");
    }

    @Test
    public void should_create_delete_command() throws Exception {
        String command = DELETE.get(103);

        assertThat(command).isEqualTo("delete 103\r\n");
    }

    @Test
    public void should_create_stats_command() throws Exception {
        String command = STATS_TUBE.get("random_tube_name");

        assertThat(command).isEqualTo("stats-tube random_tube_name\r\n");
    }

    @Test
    public void should_create_bury_command() throws Exception {
        String command = BURY.get(12l, 101);

        assertThat(command).isEqualTo("bury 12 101\r\n");
    }

    @Test
    public void should_create_release_command() throws Exception {
        String command = RELEASE.get(14l, 101, 10);

        assertThat(command).isEqualTo("release 14 101 10\r\n");
    }
}