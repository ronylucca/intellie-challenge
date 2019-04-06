package net.intelie.challenges;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class EventIteratorImplTest {

    @Test
    public void expectCurrent() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(3, eventIterator.current().timestamp());
    }
    
    @Test
    public void currentCausesExceptionWithoutMoveNext() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        try{
            eventIterator.current();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
    }
    
    @Test
    public void currentProcessThrowingExceptionUsingLimitedList() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(false, eventIterator.moveNext());
        try{
            eventIterator.current();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
    }
    
    @Test
    public void expetRemove() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        eventIterator.remove();
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromBeginingThrowingException() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        try{
            eventIterator.remove();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromBegining() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        eventIterator.remove();
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    
    
    @Test
    public void moveNextAndCurrentUsingNotEmptyList() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event, eventIterator.current());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void moveNextAndCurrentUsingEmptyList() throws Exception {
        EventStoreImpl eventStore = new EventStoreImpl();
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("some_type", 0, 125L);
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromTop() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        eventIterator.remove();
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromTopThrowingException() throws Exception {
        Event event = new Event("anytype", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("anytype", 1);
        eventStore.insert(event);
        event = new Event("anytype", 2);
        eventStore.insert(event);
        event = new Event("anytype", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        try{
            eventIterator.remove();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("anytype", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
}
