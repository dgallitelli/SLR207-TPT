public class Slave {

    public static void main(String[] args) {

        long startTime, endTime, totalTime;

        try {
            startTime = System.currentTimeMillis()/1000;
            Thread.sleep(10000);
            endTime = System.currentTimeMillis()/1000;
            totalTime = endTime-startTime;
            System.out.println("The result is "+(3+5));
            System.out.println("I waited for "+totalTime+" seconds.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}