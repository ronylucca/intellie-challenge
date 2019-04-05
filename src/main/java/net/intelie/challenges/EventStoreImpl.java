package net.intelie.challenges;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventStoreImpl implements EventStore {

	
    private List<EventIterator> eventIterators = new LinkedList();
    private HashMap<String, EventMenu> menu = new HashMap();
    private Logger logger = Logger.getLogger(EventStoreImpl.class.getName());
    
    // concurrent object
    Semaphore concurrentObject = new Semaphore(Integer.MAX_VALUE);
    Semaphore concurrentEventIterators = new Semaphore(1);

    private EventMenu getObjectListFiltered(final String type) {
        return menu.get(type);

    }
	@Override
	public void insert(Event event) {
		// TODO Auto-generated method stub
		
		try {
			//Thread safe begins
			
			   	concurrentObject.acquire(Integer.MAX_VALUE);
	        	
	        } catch (InterruptedException ex) {
	            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
	        }
	        EventMenu eventMenu = getObjectListFiltered(event.type());
	        if (eventMenu == null) {
	        	eventMenu = new EventMenu();
	            menu.put(event.type(), eventMenu);
	        }
	        concurrentObject.release(Integer.MAX_VALUE);
	        // Thread safe ends
	        
	        eventMenu.insertEvent(event);
			Semaphore sm = new Semaphore(1);
		
	}

	@Override
	public void removeAll(String type) {
		// TODO Auto-generated method stub
		try {
			//Thread safe begins
			
			concurrentObject.acquire(Integer.MAX_VALUE);
			menu.remove(type);
			    // end of thread safe to edit the event lists
			    
			concurrentObject.release(Integer.MAX_VALUE);
			
			//Thread safe ends
		}catch (InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}

	}
	

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		// TODO Auto-generated method stub
		
		try {
			//Thread safe begins
			
			concurrentObject.acquire(1);
			
			//Thread safe ends
		}catch(InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		}
		  EventMenu eventMenu = getObjectListFiltered(type);
	        concurrentObject.release(1);
	        // end of thread safe to get the Event Lists
	        EventIteratorImpl eventIterator;
	        if (eventMenu == null) {
	            eventIterator = new EventIteratorImpl(this, new LinkedList());
	            // thread safe to edit the list of iterators
	            try {
	            	concurrentEventIterators.acquire();
	            } catch (InterruptedException ex) {
	                Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
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
	
	
	 protected void closeEventIterator(EventIterator eventIerator) {
		// TODO Auto-generated method stub   
		// Thread safe begins
	        try {
	            concurrentEventIterators.acquire();
	        } catch (InterruptedException ex) {
	            logger.log(Level.SEVERE, null, ex);
	        }
	        eventIterators.remove(eventIerator);
	        concurrentEventIterators.release();
	        // Thread safe ends
	}

    protected void removeEvent(Event event) {
        //Thread safe begins
        try {
            concurrentObject.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, null, e);
        }
        EventMenu eventList = getObjectListFiltered(event.type());
        concurrentObject.release(Integer.MAX_VALUE);
        
        //Thread safe ends
        if (eventList != null) {
            eventList.removeEvent(event);
        }
    }

}
