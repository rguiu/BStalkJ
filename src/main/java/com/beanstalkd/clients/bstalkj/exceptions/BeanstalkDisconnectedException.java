package com.beanstalkd.clients.bstalkj.exceptions;

public class BeanstalkDisconnectedException extends BeanstalkException {

	public BeanstalkDisconnectedException(String message) {
		super(message);
	}
	
	public BeanstalkDisconnectedException(String message, Exception cause) {
		super(message, cause);
	}
	
	public BeanstalkDisconnectedException(Exception cause) {
		super(cause);
	}
}
