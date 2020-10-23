public class Execute{
public static void main(String args[]){
      String []b={"Apple","Mango","Orange"};
      System.out.print("Before Function Call    "+b[0]);
      ArrayDemo.passByReference(b);
      System.out.println("After Function Call    "+b[0]);
   }
}
