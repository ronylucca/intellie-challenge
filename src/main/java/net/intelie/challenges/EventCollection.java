package net.intelie.challenges;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
* This class is responsible for encapsulate to avoid any dirty code smell :)
* @author  Rony de Lucca
* @see  Event
* @see  LinkedList
* @see  ConcurrenctObject
* @see  Map
*/
public class EventCollection {
	
	//Faster to filter from type<String>
    private final List<Event> events = new LinkedList<Event>();
    
    //I would implement a pojo to represent the machine state and some crazy stuff, then I prefer use some utils package candies. 
    private Semaphore concurrentObject = new Semaphore(Integer.MAX_VALUE);
    
    private Logger logger = Logger.getLogger(EventStoreImpl.class.getName());
    
    private static int PERMIT_ONE = 1;
    private static int PERMIT_MAX = Integer.MAX_VALUE;
    
    
    
    public void insertEvent(Event event) {
        
        try {
            concurrentObject.acquire(PERMIT_MAX);
        } catch (InterruptedException ex) {
        	logger.log(Level.SEVERE, null, ex);
        }
        
        events.add(searchObject(event.timestamp()), event);
        concurrentObject.release(PERMIT_MAX);
    
    }
    
    public void removeItem(Event event) {
        // Thread safe begins
        try {
            concurrentObject.acquire(PERMIT_MAX);
        } catch (InterruptedException e) {
        	logger.log(Level.SEVERE, null, e);
        }
        events.remove(event);
        concurrentObject.release(PERMIT_MAX);
        // Thread safe ends
    }
    
    public  List<Event> query(long startTime, long endTime) {
    	
        try {
            concurrentObject.acquire(PERMIT_ONE);
        } catch (InterruptedException e) {
        	logger.log(Level.SEVERE, null, e);
        }
        
        int initialIndex = searchObject(startTime);
        int finalIndex = searchObject(endTime-1);
        boolean found = false;
        
        //calibrateInitialIndex(found, initialIndex, startTime);
        while(!found){
            if(initialIndex > 0 && events.get(initialIndex - 1).timestamp() == startTime) {
                initialIndex--;
            } else {
                found = true;
            }
        }
        
        
        //calibrateFinalIndex(found, finalIndex, startTime, endTime);
        found = false;
        int length = events.size();
        while(!found){
            if(finalIndex < length && events.get(finalIndex).timestamp() == (endTime-1)) {
            	finalIndex++;
            } else {
                found = true;
            }
        }
        
        
        List<Event> list = new LinkedList<Event>(events.subList(initialIndex, finalIndex));
        concurrentObject.release(PERMIT_ONE);
        
        return list;
    }
    
    
    //Method responsible for search event
    private int searchObject(final long timestamp) {
        if(events.isEmpty()) {
            return 0;
        }
        int start = 0;
        int central;
        int end = events.size() - PERMIT_ONE;
        
        while(start < end) {
            central = Integer.divideUnsigned(Integer.sum(end, start), 2);
            long searchTimestamp = events.get(central).timestamp();
            if(timestamp == searchTimestamp){
                return central;
            } else if (timestamp > searchTimestamp) {
                start = Integer.sum(central, PERMIT_ONE);
            } else {
                end = central - 1;
            }
        }
        if(events.get(start).timestamp() > timestamp) {
            return start;
        }
        return start+1;
    }
    
    /*
     * just following some clean code slicing caller method. 
     * method responsible for set the initial pointer(index) in List
     * Update: during tests, this way I tried wasn't well. 
     * Then I realized the issue was regarding the memory state of all vars inside.
     */
    @Deprecated
    private void calibrateInitialIndex(boolean found, int initialIndex, long startTime) {
    	
    	while(!found){
            if(initialIndex > 0 && events.get(initialIndex - 1).timestamp() == startTime) {
                initialIndex--;
            } else {
                found = true;
            }
        }
    }
    
    /*
     * just following some clean code slicing caller method as before
     * method responsible for set the final pointer(index) in List
     * Update: during tests, this way I tried wasn't well. 
     * Then I realized the issue was regarding the memory state of all vars inside.
     */
    @Deprecated
    private void calibrateFinalIndex(boolean found, int finalIndex, long startTime, long endTime) {
    	
    	found = false;
        int length = events.size();
        while(!found){
            if(finalIndex < length && events.get(finalIndex).timestamp() == (endTime-1)) {
            	finalIndex++;
            } else {
                found = true;
            }
        }
    }
    
    
    
    
    
}