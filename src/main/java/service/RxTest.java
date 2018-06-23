package service;

import rx.Observable;

public class RxTest {

    public static void main(String[] args) {
RxTest obj = new RxTest();
Observable<Integer> obs = obj.doSomething();
obs.filter( x -> x<9).map(x-> x.toString() + "a").subscribe(x-> System.out.println(x));


    }


    public Observable<Integer> doSomething(){
        Observable<Integer> obs = Observable.range(1,10);
        return obs;

    }

}
