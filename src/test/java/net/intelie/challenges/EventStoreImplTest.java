package net.intelie.challenges;

import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class EventStoreImplTest {



	@Test
	public void executeQuery() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		EventIterator eventIterator = eventStore.query("anytype", 0, 125L);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
	}


	@Test
	public void queryWithDuplicatedExcludeAtEnd() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		
		eventStore.insert(event);
		event = new Event("anytype", 12);
		
		eventStore.insert(event);
		event = new Event("anytype", 5);
		
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		EventIterator eventIterator = eventStore.query("anytype", 0, 51);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		
	}

	@Test
	public void queryWithMultipleIncludesAtTheEnd() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 51);
		eventStore.insert(event);
		event = new Event("anytype", 51);
		eventStore.insert(event);
		event = new Event("anytype", 51);
		eventStore.insert(event);
		event = new Event("anytype", 51);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		event = new Event("anytype", 50);
		eventStore.insert(event);
		
		EventIterator eventIterator = eventStore.query("anytype", 0, 51);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(50, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(50, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(50, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(50, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(50, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(50, eventIterator.current().timestamp());
		assertEquals(false, eventIterator.moveNext());
	}

	@Test
	public void queryWithDuplicatedAtStart() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
				eventStore.insert(event);
		event = new Event("anytype", 1);
		eventStore.insert(event);
		event = new Event("anytype", 2);
		eventStore.insert(event);
		event = new Event("anytype", 43);
		eventStore.insert(event);
		event = new Event("anytype", 21);
		eventStore.insert(event);
		event = new Event("anytype", 3);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 8);
		eventStore.insert(event);
		
		EventIterator eventIterator = eventStore.query("anytype", 5, 13);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(8, eventIterator.current().timestamp());
		assertEquals(false, eventIterator.moveNext());
	}

	@Test
	public void queryWithDuplicatedAtOneBeforeBegin() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		eventStore.insert(event);
		event = new Event("anytype", 56);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 123);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 37);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 91);
		eventStore.insert(event);
		event = new Event("anytype", 92);
		eventStore.insert(event);
		
		EventIterator eventIterator = eventStore.query("anytype", 90, 123);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(91, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(92, eventIterator.current().timestamp());
		assertEquals(false, eventIterator.moveNext());
	}

	@Test
	public void uniqueInsert() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		eventStore.insert(event);
		EventIterator eventIterator = eventStore.query("anytype", 0, 125L);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(event, eventIterator.current());
		assertEquals(false, eventIterator.moveNext());
	}

	@Test
	public void bulkInsert() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		eventStore.insert(event);
		event = new Event("anytype", 56);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 37);
		eventStore.insert(event);
		EventIterator eventIterator = eventStore.query("anytype", 0, 125L);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(37, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(56, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(89, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(123, eventIterator.current().timestamp());
		assertEquals(false, eventIterator.moveNext());
	}

	@Test
	public void multipleInsertWithDuplicatedtimestamp() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		eventStore.insert(event);
		event = new Event("anytype", 56);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 123);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 37);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		EventIterator eventIterator = eventStore.query("anytype", 0, 125L);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(37, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(56, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(89, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(89, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(123, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(123, eventIterator.current().timestamp());
		assertEquals(false, eventIterator.moveNext());
	}

	@Test
	public void executeRemoveAll() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		eventStore.insert(event);
		event = new Event("anytype", 56);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 37);
		eventStore.insert(event);
		eventStore.removeAll("anytype");
		EventIterator eventIterator = eventStore.query("anytype", 0, 125L);
		assertEquals(false, eventIterator.moveNext());
	}

	@Test
	public void removeAllFromOddType() throws Exception {
		Event event = new Event("anytype", 123L);
		EventStoreImpl eventStore = new EventStoreImpl();
		eventStore.insert(event);
		event = new Event("anytype", 56);
		eventStore.insert(event);
		event = new Event("anytype", 12);
		eventStore.insert(event);
		event = new Event("anytype", 89);
		eventStore.insert(event);
		event = new Event("anytype", 5);
		eventStore.insert(event);
		event = new Event("anytype", 4);
		eventStore.insert(event);
		event = new Event("anytype", 37);
		eventStore.insert(event);
		eventStore.removeAll("som_type");
		EventIterator eventIterator = eventStore.query("anytype", 0, 125L);
		assertEquals(true, eventIterator.moveNext());
		assertEquals(4, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(5, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(12, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(37, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(56, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(89, eventIterator.current().timestamp());
		assertEquals(true, eventIterator.moveNext());
		assertEquals(123, eventIterator.current().timestamp());
		assertEquals(false, eventIterator.moveNext());
	}
}
