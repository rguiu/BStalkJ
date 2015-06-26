package com.beanstalkd.clients.bstalkj.exceptions;

public class BeanstalkException extends RuntimeException {

	private static final long serialVersionUID = -7534715964776241767L;

	public BeanstalkException(String message) {
		super(message);
	}
	
	public BeanstalkException(String message, Exception cause) {
		super(message, cause);
	}
	
	public BeanstalkException(Exception cause) {
		super(cause);
	}
}
