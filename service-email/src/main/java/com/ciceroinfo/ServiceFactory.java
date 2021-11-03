package com.ciceroinfo;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
