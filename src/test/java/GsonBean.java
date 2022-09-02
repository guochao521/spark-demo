import java.util.ArrayList;

public class GsonBean {

    public GsonBean() {
    }
    public GsonBean(int num, ArrayList<Account> data) {
        super();
        this.num = num;
        this.data = data;
    }
    public int num;
    public ArrayList<Account> data;
    /**data数组里的对象*/
    public class Account {
        public String username;
        public ArrayList<Person> friend;
        @Override
        public String toString() {
            return "账户【username=" + username + "，friend=" + friend + "】";
        }
        public Account(String username, ArrayList<Person> friend) {
            super();
            this.username = username;
            this.friend = friend;
        }
    }
    @Override
    public String toString() {
        return "Gson 【num=" + num + "，data=" + data + "】";
    }
}
