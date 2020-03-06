package com.datastax.samples.dto;

import java.io.Serializable;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.samples.ExampleSchema;

/**
 * Sample bean for row.
 */
public class UserDto implements Serializable, ExampleSchema {
    
    /** Serial. */
    private static final long serialVersionUID = -6767335554891314036L;

    private String email;
    
    private String firstName;
    
    private String lastName;

    public UserDto() {
    }
    
    public UserDto(Row tableUsersRow) {
        super();
        this.email      = tableUsersRow.getString(USER_EMAIL);
        this.firstName  = tableUsersRow.getString(USER_FIRSTNAME);
        this.lastName   = tableUsersRow.getString(USER_LASTNAME);
    }
    
    public UserDto(String email, String firstName, String lastName) {
        super();
        this.email = email;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * Getter accessor for attribute 'email'.
     *
     * @return
     *       current value of 'email'
     */
    public String getEmail() {
        return email;
    }

    /**
     * Setter accessor for attribute 'email'.
     * @param email
     * 		new value for 'email '
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * Getter accessor for attribute 'firstName'.
     *
     * @return
     *       current value of 'firstName'
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * Setter accessor for attribute 'firstName'.
     * @param firstName
     * 		new value for 'firstName '
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * Getter accessor for attribute 'lastName'.
     *
     * @return
     *       current value of 'lastName'
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * Setter accessor for attribute 'lastName'.
     * @param lastName
     * 		new value for 'lastName '
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

}
