import java.util.ArrayList;
import java.util.Collections;
public class MyClasss {
  public static void main(String[] args) {
	ArrayList<Integer> cars = new ArrayList<Integer>();
	//for(int i=0;i<7;i++) {
	//cars.add(i);}
	cars.add(1);
	cars.add(3);
	cars.add(2);
	Collections.sort(cars);
	for(int i:cars) {	
	System.out.println(i);}
  }
}
