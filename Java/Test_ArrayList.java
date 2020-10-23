import java.util.ArrayList;
class Test_ArrayList {
	public static void main(String[] args) {
		ArrayList<String> arlTest = new ArrayList<String>();
		System.out.println("Size of ArrayList as creation: " + arlTest.size());
		arlTest.add("a");
		arlTest.add("r");
		arlTest.add("v");
		arlTest.add("i");
		arlTest.add("n");
		arlTest.add("d");
		System.out.println("Size of ArrayList as creation: " + arlTest.size());
		System.out.println("Elements:" + arlTest);
		arlTest.remove("d");
  		System.out.println("See contents after removing one element: " + arlTest);
		arlTest.remove(2);
  		System.out.println("See contents after removing element by index: " + arlTest);
		System.out.println("Size of arrayList after removing elements: " + arlTest.size());
		System.out.println("List of all elements after removing elements: " + arlTest);
		System.out.println(arlTest.contains("v"));
	}
}
