
import java.sql.*;
import java.util.*;

public class PhoenixService {
    private static Connection conn;

    // 初始化链接
    public static void init() {
        try {
            String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
            Class.forName(driver);
            conn = DriverManager.getConnection("jdbc:phoenix:10.110.181.39:2181:/hbase-unsecure");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 关闭连接
    public static void close() {
        try {
            if (null != conn) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void executeUpdate(String sql) {
        init();
        try {
            Statement sm = conn.createStatement();
            sm.executeUpdate(sql);
            conn.commit();
            if (null != sm) {
                sm.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        close();
    }

    private static List converList(ResultSet rs) throws SQLException {
        List list = new ArrayList();
        ResultSetMetaData md = rs.getMetaData();    //获取键名
        int colCount = md.getColumnCount();         //获取行数
        while (rs.next()) {
            Map rowData = new HashMap();
            for (int i=1; i<=colCount; i++) {
                rowData.put(md.getColumnName(i), rs.getObject(i));//获取键名及值
            }
            list.add(rowData);
        }
        return list;
    }

    public static List<ResultSet> executeQuery(String sql) {
        init();
         List<ResultSet> res = new ArrayList<ResultSet>();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            res = converList(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        close();
        return res;
    }

    public static void main(String[] args) {
        init();
        String sql_drop = "drop table if exists \"wangjj\"";
        String sql_create = "create table \"wangjj\" (\"id\" integer not null primary key, \"name\" varchar, \"age\" integer)";
        String sql_upsert = "upsert into \"wangjj\" values (1, 'jacky', 20)";
        String sql_select = "select * from \"wangjj\"";
        StringBuilder sb = new StringBuilder();
        sb.append(sql_drop+";");
        sb.append(sql_create+";");
        sb.append(sql_upsert+";");
        sb.append(sql_select);
        String[] sqls = sb.toString().split(";");
        for (int i=0; i<sqls.length; i++) {
            String sql = sqls[i];
            System.out.println(sql);
            if (sql.split(" ")[0].equals("select")) {
                List res = executeQuery(sql);
                for (int j=0; j<res.size(); j++) {
                    Map rowData = (Map) res.get(j);
                    for (Object key : rowData.keySet()) {
                        System.out.print(key.toString()+":"+rowData.get(key).toString()+" ");
                    }
                    System.out.println();
                }
            } else {
                executeUpdate(sql);
            }
        }
        close();
    }
}