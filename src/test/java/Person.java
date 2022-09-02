public class Person {

    //不要求一定有get、set方法，也不要求一定有无参构造方法，甚至不要求其成员是public还是private
    //但要求所有字段名必须和json字符串中的一致
    public String name;
    public String address;
    public Person(String name, String address) {
        this.name = name;
        this.address = address;
    }
    @Override
    public String toString() {
        return "name=" + name + " & " + "address=" + address;
    }
}
