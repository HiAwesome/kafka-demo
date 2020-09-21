package com.moqi.bean;

/**
 * Customer Bean 类
 *
 * @author moqi
 * On 9/21/20 16:40
 */

public class Customer {

    private final int customerID;
    private final String customerName;

    public Customer(int ID, String name) {
        this.customerID = ID;
        this.customerName = name;
    }

    public int getID() {
        return customerID;

    }

    public String getName() {
        return customerName;
    }

}
