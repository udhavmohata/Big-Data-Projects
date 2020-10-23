abstract class Person {
	public String fname = "John";
	public int age = 24;
	public abstract void study();
}

class Students extends Person {
	public int graduationYear = 2018;
	public void study(){
		System.out.println("Studying all day Long");
	}	
}
class MyClass {
	public static void main(String[] args) {
		Students myObj = new Students();
		System.out.println("Name: " + myObj.fname);
		System.out.println("Age: " + myObj.age);
		System.out.println("Graduation Year: " + myObj.graduationYear);
		myObj.study();
	}
}
