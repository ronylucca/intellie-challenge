package net.intelie.challenges;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EventCollectionTest {

	
	@Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }
}
