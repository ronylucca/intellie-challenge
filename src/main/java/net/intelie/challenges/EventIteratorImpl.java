package net.intelie.challenges;

import java.util.List;

public class EventIteratorImpl implements EventIterator {

    private final List<Event> events;
    private final EventStoreImpl eventStore;
    private int pointer = -1;
    
    EventIteratorImpl(EventStoreImpl eventStore, List<Event> events) {
        this.eventStore = eventStore;
        this.events = events;
    }
    
    
	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		eventStore.closeEventIterator(this);

	}

	@Override
	public boolean moveNext() {
		// TODO Auto-generated method stub
		pointer++;
        if(pointer >= events.size()) {
            return false;
        }
        return true;
	}

	@Override
	public Event current() {
		// TODO Auto-generated method stub
		if(pointer == -1 || pointer >= events.size()) {
            throw new IllegalStateException();
        }
        return events.get(pointer);
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

		if(pointer == -1 || pointer >= events.size()) {
            throw new IllegalStateException();
        }
        eventStore.removeEvent(events.get(pointer));
        events.remove(pointer);
		
	}

}
