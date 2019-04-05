package net.intelie.challenges;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventMenu {
	
    private final List<Event> events = new LinkedList<Event>();
    
    private Semaphore concurrentObject = new Semaphore(Integer.MAX_VALUE);
    
    private Logger logger = Logger.getLogger(EventStoreImpl.class.getName());
    
    private int searchObject(final long timestamp) {
        if(events.isEmpty()) {
            return 0;
        }
        int begin = 0;
        int end = events.size() - 1;
        int middle;
        while(end > begin) {
            middle = (end + begin)/2;
            long searchTimestamp = events.get(middle).timestamp();
            if(timestamp == searchTimestamp){
                return middle;
            } else if (timestamp > searchTimestamp) {
                begin = middle+1;
            } else {
                end = middle - 1;
            }
        }
        if(events.get(begin).timestamp() > timestamp) {
            return begin;
        }
        return begin + 1;
    }
    
    public void insertEvent(Event event) {
        
        try {
            concurrentObject.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException ex) {
        	logger.log(Level.SEVERE, null, ex);
        }
        
        events.add(searchObject(event.timestamp()), event);
        concurrentObject.release(Integer.MAX_VALUE);
    
    }
    
    public void removeEvent(Event event) {
        // thread safe to edit the list
        try {
            concurrentObject.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException ex) {
        	logger.log(Level.SEVERE, null, ex);
        }
        events.remove(event);
        concurrentObject.release(Integer.MAX_VALUE);
        // end thread safe to edit the list
    }
    
    public  List<Event> query(long startTime, long endTime) {
    	
        try {
            concurrentObject.acquire(1);
        } catch (InterruptedException ex) {
        	logger.log(Level.SEVERE, null, ex);
        }
        
        int indexBegin = searchObject(startTime);
        int indexEnd = searchObject(endTime-1);
        boolean found = false;
        while(!found){
            if(indexBegin > 0 && events.get(indexBegin-1).timestamp() == startTime) {
                indexBegin--;
            } else {
                found = true;
            }
        }
        found = false;
        int length = events.size();
        while(!found){
            if(indexEnd < length && events.get(indexEnd).timestamp() == (endTime-1)) {
                indexEnd++;
            } else {
                found = true;
            }
        }
        List<Event> list = new LinkedList<Event>(events.subList(indexBegin, indexEnd));
        concurrentObject.release(1);
        
        return list;
    }
    
}