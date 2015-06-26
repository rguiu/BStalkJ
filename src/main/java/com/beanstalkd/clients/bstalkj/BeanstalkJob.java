package com.beanstalkd.clients.bstalkj;

import lombok.Getter;
import lombok.Builder;

@Builder
@Getter
public class BeanstalkJob {
	private byte[] data;
	private long id;
}
