@FunctionalInterface
 interface A{
    void call();
}

public class TestJava{
    public static void main(String[] args){
        runThreadByLambda();
        runThreadByInnerClass();
    }

    public static void runThreadByLambda() {
        Runnable r=()->System.out.println("This is a lambda");
        new Thread(r).start();
    }

    public static void runThreadByInnerClass() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println("inner class");
            }
        };
        new Thread(runnable).start();
    }
}
