package net.intelie.challenges;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Class responsible for implements all Event statements
 *  
 */
public class EventStoreImpl implements EventStore {

	//Performance improved using LinkedList. Easier to manipulate the process state(Insert, Remove, Search{get}) in multiple instances(parallelism)
	private List<EventIterator> eventIterators = new LinkedList<EventIterator>();
	private HashMap<String, EventCollection> menu = new HashMap<String, EventCollection>();
	private Logger logger = Logger.getLogger(EventStoreImpl.class.getName());
	private static int PERMIT_ONE = 1;
	private static int PERMIT_MAX = Integer.MAX_VALUE;

	// concurrent object
	Semaphore concurrentObject = new Semaphore(PERMIT_MAX);
	Semaphore concurrentEventIterators = new Semaphore(PERMIT_ONE);

    
    private EventCollection getObjectListFilteredByType(final String type) {
    	return menu.get(type);

    }
    
    
	@Override
	public void insert(Event event) {
		// TODO Auto-generated method stub
		try {
			//Thread safe begins

			concurrentObject.acquire(PERMIT_MAX);

		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}
		EventCollection eventMenu = getObjectListFilteredByType(event.type());
		if (eventMenu == null) {
			eventMenu = new EventCollection();
			menu.put(event.type(), eventMenu);
		}
		concurrentObject.release(PERMIT_MAX);
		// Thread safe ends

		eventMenu.insertEvent(event);
		

	}

	@Override
	public void removeAll(String type) {
		// TODO Auto-generated method stub
		try {
			//Thread safe begins

			concurrentObject.acquire(PERMIT_MAX);
			
			//Thread safe ends
		}catch (InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}
		
		menu.remove(type);
		concurrentObject.release(PERMIT_MAX);

	}
	

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		// TODO Auto-generated method stub
		try {
			//Thread safe begins

			concurrentObject.acquire(PERMIT_ONE);

			//Thread safe ends
		}catch(InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}
		EventCollection eventMenu = getObjectListFilteredByType(type);
		concurrentObject.release(PERMIT_ONE);
		// end of thread safe to get the Event Lists
		EventIteratorImpl eventIterator;
		if (eventMenu == null) {
			eventIterator = new EventIteratorImpl(this, new LinkedList());
			// thread safe to edit the list of iterators
			try {
				concurrentEventIterators.acquire();
			} catch (InterruptedException e) {
				logger.log(Level.SEVERE, null, e);
			}
			eventIterators.add(eventIterator);
			concurrentEventIterators.release();
			// end of thread safe to edit the list of iterators
			return eventIterator;
		}

		List<Event> list = eventMenu.query(startTime, endTime);
		eventIterator = new EventIteratorImpl(this, list);
		// Thread safe begins
		
		try {
			concurrentEventIterators.acquire();
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}
		eventIterators.add(eventIterator);
		concurrentEventIterators.release();

		// Thread safe ends for iterators

		return eventIterator;

	}
	
	/*
	 * Method used by encapsulated calls from EventCollection
	 */
	protected void closeEventIterator(EventIterator eventIerator) {
		// TODO Auto-generated method stub   
		// Thread safe begins
		try {
			concurrentEventIterators.acquire();
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}
		eventIterators.remove(eventIerator);
		concurrentEventIterators.release();
		// Thread safe ends
	}

    
    /*
	 * Method used by encapsulated calls from EventCollection
	 */
	protected void removeEvent(Event event) {
		//Thread safe begins
		try {
			concurrentObject.acquire(PERMIT_MAX);
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}
		EventCollection eventList = getObjectListFilteredByType(event.type());
		concurrentObject.release(PERMIT_MAX);

		//Thread safe ends
		if (eventList != null) {
			eventList.removeItem(event);
		}
	}

}
