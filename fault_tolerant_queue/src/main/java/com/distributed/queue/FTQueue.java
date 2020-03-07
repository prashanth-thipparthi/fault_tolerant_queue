package fault_tolerant_queue.src.main.java.com.distributed.queue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class FTQueue {
    private TreeMap<Integer, Queue<Integer>> queueIdsToQueueMap = new TreeMap<>();
    private Map<Integer, Integer> queueLabelsToQueueId = new HashMap<>();

    public String qCreate(int label){
        if(queueLabelsToQueueId.containsKey(label)){
            return "Queue with label "+label + " already exists, queueId: "  + Integer.toString(queueLabelsToQueueId.get(label));
        }

        Queue<Integer> queue = new LinkedList<>();
        int lastKey = queueIdsToQueueMap.lastKey();
        queueIdsToQueueMap.put(lastKey+1,queue);
        queueLabelsToQueueId.put(label, lastKey+1);
        return "Queue with label "+label + " created with queueId: " + lastKey+1;

    }
    public String qDestroy(int queue_id){  //deletes a queue
        if(! queueLabelsToQueueId.containsValue(queue_id)){
            return "Queue with id "+ queue_id + " does not exist";
        }

        int q_label = 0;
        for(Map.Entry<Integer, Integer> entry : queueLabelsToQueueId.entrySet()) {
            if(entry.getValue() == queue_id) {
                q_label = entry.getKey();
            }
        }
        queueIdsToQueueMap.remove(queue_id);
        queueLabelsToQueueId.remove(q_label);
        return "Queue with Id: "+ queue_id +"destroyed";
    }

    /*     returns queue id of the queue associated with label if one exists Otherwise, return -1     */
    public String qId (int label)  {
        if(! queueLabelsToQueueId.containsKey(label)){
            return "Queue with label "+ label + " does not exist";
        }

        return "Queue with label "+ label + " is " +queueLabelsToQueueId.get(label);
    }
    /*  enters item in the queue */ 
    public String qPush (int queue_id, int item){
        if(!queueIdsToQueueMap.containsKey(queue_id)) {
            return "Queue with Id "+ queue_id + " does not exist";
        }

        queueIdsToQueueMap.get(queue_id).offer(item);
        return "Element" + item + " is pushed into the queue:" +queue_id;
    }
    /*    removes an item from the queue and returns it     */
    public String qPop (int queue_id){
        if(!queueIdsToQueueMap.containsKey(queue_id)) {
            return "Queue with Id "+ queue_id + " does not exist";
        }

        int polled = queueIdsToQueueMap.get(queue_id).poll();
        return "Element" + polled + " is poped from the queue:" +queue_id;
    }
    /*    returns the value of the first element in the queue     */
    public String qTop (int queue_id)  {
        if(!queueIdsToQueueMap.containsKey(queue_id)) {
            return "Queue with Id "+ queue_id + " does not exist";
        }

        int top = queueIdsToQueueMap.get(queue_id).peek();
        return "Element" + top + " is at the top of the queue:" +queue_id;
    }

    /*     returns the number of items in the queue     */
    public String qSize (int queue_id)  {
        if(!queueIdsToQueueMap.containsKey(queue_id)) {
            return "Queue with Id "+ queue_id + " does not exist";
        }

        int size = queueIdsToQueueMap.get(queue_id).size();
        return size + " is the size of the queue:" +queue_id;
    } 
}