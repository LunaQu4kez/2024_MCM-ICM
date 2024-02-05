import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class DataReader {
    private Connection con = null;
    private PreparedStatement stmt = null;

    private String host = "localhost";
    private String dbname = "2024MCMICM";
    private String user = "postgres";
    private String pwd = "123456";
    private String port = "5432";

    public static void main(String[] args) {
        DataReader reader = new DataReader();
        reader.openDB();
        reader.truncateDisaster();
        int year = 1950;
        for (; year <= 2023; year++) {
            String filePath = "data\\" + year + ".csv";
            System.out.println(year);
            reader.insert(filePath);
        }
        reader.closeDB();
    }

    private void openDB() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
        try {
            con = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private void closeDB() {
        if (con != null) {
            try {
                con.close();
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void truncateDisaster() {
        String sql = "truncate table disaster cascade";
        try {
            stmt = con.prepareStatement(sql);
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insert(String filePath) {
        try {
            CSVReader sc = new CSVReader(new FileReader(filePath));
            String[] data;
            sc.readNext();
            while ((data = sc.readNext()) != null) {
                int year = Integer.parseInt(data[0].substring(0, 4));
                int month = Integer.parseInt(data[0].substring(4, 6));
                int day = Integer.parseInt(data[1]);
                String state = data[8];
                String type = data[12];
                String damage_prop_str = data[24];
                long damage_prop = 0;
                if (damage_prop_str != null && damage_prop_str.length() > 0) {
                    if (damage_prop_str.charAt(0) == '.') {
                        damage_prop_str = "0" + damage_prop_str;
                    }
                    if (damage_prop_str.length() > 1) {
                        double d = Double.parseDouble(damage_prop_str.substring(0, damage_prop_str.length() - 1));
                        if (damage_prop_str.charAt(damage_prop_str.length() - 1) == 'K') {
                            damage_prop = (long) (d * 1000);
                        } else if (damage_prop_str.charAt(damage_prop_str.length() - 1) == 'M') {
                            damage_prop = (long) (d * 1000000);
                        }
                    }
                }
                String sql = "insert into disaster values (?,?,?,?,?,?)";
                stmt = con.prepareStatement(sql);
                stmt.setInt(1, year);
                stmt.setInt(2, month);
                stmt.setInt(3, day);
                stmt.setString(4, state);
                stmt.setString(5, type);
                stmt.setLong(6, damage_prop);
                stmt.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
