package com.example.kafkasample05.common;

/**
 * @Author: likewater
 * @Description:
 * @Date: Create in 11:06 上午 2022/2/13
 */
public class Foo1 {
    private String foo;

    public Foo1() {
    }

    public Foo1(String foo) {
        this.foo = foo;
    }

    public String getFoo() {
        return this.foo;
    }

    public void setFoo(String foo) {
        this.foo = foo;
    }

    @Override
    public String toString() {
        return "Foo1 [foo=" + this.foo + "]";
    }
}
