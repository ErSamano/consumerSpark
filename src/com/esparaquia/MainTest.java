package com.esparaquia;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {
    
    @Test
    public void testPrintMessage() {

        int nym = Main.GetRandomNumber();
        System.out.println("Test " + nym);
        assertTrue(nym <5000);
    }


}