package com.beanstalkd.clients.bstalkj;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beanstalkd.clients.bstalkj.exceptions.BeanstalkException;

public class BeanstalkProtocol {
    private static Logger log = LoggerFactory.getLogger(BeanstalkProtocol.class);

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 11300;
    public static final int DEFAULT_TIMEOUT = 2000;
    public static final int DEFAULT_SO_TIMEOUT = 10000;
    public static final String DEFAULT_TUBE = "default";

    public enum Command {
        USE("use %s\r\n", "^USING.*"),
        WATCH("watch %s\r\n", "^WATCHING.*"),
        IGNORE("ignore %s\r\n", "^WATCHING.*"),
        PUT("put %s %s %s %s\r\n", "^INSERTED.*"),
        RESERVE("reserve\r\n", "^(RESERVED|TIMEOUT).*"),
        RESERVE_WITH_TIMEOUT("reserve-with-timeout %s\r\n", "^(RESERVED|TIMEOUT).*"),
        DELETE("delete %s\r\n", "^DELETED.*"),
        STATS_TUBE("stats-tube %s\r\n", "^OK.*"),
        BURY("bury %s %s\r\n", "^BURIED.*"),
        RELEASE("release %s %s %s\r\n", "^RELEASED.*");

        private String template;
        protected Pattern validResponsePattern;

        Command(String template, String validResponseRegexp) {
            this.template = template;
            this.validResponsePattern = Pattern.compile(validResponseRegexp);
        }

        public String get(Object... values) {
            return String.format(template, values);
        }

        public void check(String response) {
            if (response == null) {
                throw new BeanstalkException("Response is null");
            }
            Matcher matcher = validResponsePattern.matcher(response);
            if (!matcher.matches()) {
                log.error("{}:{}:{}", this.name(), response.trim(), validResponsePattern);
                throw new BeanstalkException(response);
            }
        }
    }
}
