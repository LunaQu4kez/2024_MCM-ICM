import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class FileSeperator {
    static String[] states = {
            "ALABAMA",
            "ALASKA",
            "AMERICAN SAMOA",
            "ARIZONA",
            "ARKANSAS",
            "ATLANTIC NORTH",
            "ATLANTIC SOUTH",
            "CALIFORNIA",
            "COLORADO",
            "CONNECTICUT",
            "DELAWARE",
            "DISTRICT OF COLUMBIA",
            "E PACIFIC",
            "FLORIDA",
            "GEORGIA",
            "GUAM",
            "GULF OF ALASKA",
            "GULF OF MEXICO",
            "HAWAII WATERS",
            "HAWAII",
            "IDAHO",
            "ILLINOIS",
            "INDIANA",
            "IOWA",
            "KANSAS",
            "Kentucky",
            "KENTUCKY",
            "LAKE ERIE",
            "LAKE HURON",
            "LAKE MICHIGAN",
            "LAKE ONTARIO",
            "LAKE ST CLAIR",
            "LAKE SUPERIOR",
            "LOUISIANA",
            "MAINE",
            "MARYLAND",
            "MASSACHUSETTS",
            "MICHIGAN",
            "MINNESOTA",
            "MISSISSIPPI",
            "MISSOURI",
            "MONTANA",
            "NEBRASKA",
            "NEVADA",
            "NEW HAMPSHIRE",
            "NEW JERSEY",
            "NEW MEXICO",
            "NEW YORK",
            "NORTH CAROLINA",
            "NORTH DAKOTA",
            "OHIO",
            "OKLAHOMA",
            "OREGON",
            "PENNSYLVANIA",
            "PUERTO RICO",
            "RHODE ISLAND",
            "SOUTH CAROLINA",
            "SOUTH DAKOTA",
            "ST LAWRENCE R",
            "TENNESSEE",
            "TEXAS",
            "UTAH",
            "VERMONT",
            "VIRGIN ISLANDS",
            "VIRGINIA",
            "WASHINGTON",
            "WEST VIRGINIA",
            "WISCONSIN",
            "WYOMING"
    };

    static String[] types = {
            "Astronomical Low Tide",
            "Avalanche",
            "Blizzard",
            "Coastal Flood",
            "Cold/Wind Chill",
            "Debris Flow",
            "Dense Fog",
            "Dense Smoke",
            "Drought",
            "Dust Devil",
            "Dust Storm",
            "Excessive Heat",
            "Extreme Cold/Wind Chill",
            "Flash Flood",
            "Flood",
            "Freezing Fog",
            "Frost/Freeze",
            "Funnel Cloud",
            "HAIL FLOODING",
            "Hail",
            "HAIL/ICY ROADS",
            "Heat",
            "Heavy Rain",
            "Heavy Snow",
            "High Surf",
            "High Wind",
            "Hurricane (Typhoon)",
            "Hurricane",
            "Ice Storm",
            "Lake-Effect Snow",
            "Lakeshore Flood",
            "Lightning",
            "Marine Dense Fog",
            "Marine Hail",
            "Marine High Wind",
            "Marine Hurricane/Typhoon",
            "Marine Lightning",
            "Marine Strong Wind",
            "Marine Thunderstorm Wind",
            "Marine Tropical Depression",
            "Marine Tropical Storm",
            "Northern Lights",
            "Rip Current",
            "Seiche",
            "Sleet",
            "Sneakerwave",
            "Storm Surge/Tide",
            "Strong Wind",
            "Thunderstorm Wind",
            "THUNDERSTORM WIND/ TREE",
            "THUNDERSTORM WIND/ TREES",
            "THUNDERSTORM WINDS FUNNEL CLOU",
            "THUNDERSTORM WINDS HEAVY RAIN",
            "THUNDERSTORM WINDS LIGHTNING",
            "THUNDERSTORM WINDS/ FLOOD",
            "THUNDERSTORM WINDS/FLASH FLOOD",
            "THUNDERSTORM WINDS/FLOODING",
            "THUNDERSTORM WINDS/HEAVY RAIN",
            "Tornado",
            "TORNADO/WATERSPOUT",
            "TORNADOES & TSTM WIND & HAIL",
            "Tropical Depression",
            "Tropical Storm",
            "Tsunami",
            "Volcanic Ash",
            "Volcanic Ashfall",
            "Waterspou",
            "Wildfire",
            "Winter Storm",
            "Winter Weather"
    };

    private static Connection con = null;
    private static PreparedStatement stmt = null;
    private static ResultSet rs = null;

    private String host = "localhost";
    private String dbname = "2024MCMICM";
    private String user = "postgres";
    private String pwd = "123456";
    private String port = "5432";

    public static void main(String[] args) {
        FileSeperator f = new FileSeperator();
        f.openDB();
        String firstLine = ",";
        for (int j = 0; j < types.length; j++) {
            firstLine += types[j] + ",";
        }
        firstLine = firstLine.substring(0, firstLine.length() - 1);
        for (int i = 0; i < states.length; i++) {
            String filepath = "data_by_state\\" + states[i] + ".csv";
            new File(filepath);
            try {
                FileWriter fw = new FileWriter(filepath);
                fw.write(firstLine + "\n");
                int[][] ans = getAns(states[i]);
                for (int j = 0; j < ans.length; j++) {
                    String line = (j + 1950) + ",";
                    for (int k = 0; k < ans[0].length; k++) {
                        line += ans[j][k] + ",";
                    }
                    fw.write(line.substring(0, line.length() - 1) + "\n");
                }
                fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        f.closeDB();
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
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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

    private static int[][] getAns(String state) {
        int[][] ans = new int[74][types.length];
        String sql = "select type, year, count(*) from disaster where state = ? group by type, year";
        try {
            stmt = con.prepareStatement(sql);
            stmt.setString(1, state);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String type = rs.getString(1);
                int year = Integer.parseInt(rs.getString(2));
                int num = Integer.parseInt(rs.getString(3));
                ans[year-1950][type2num(type)] = num;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ans;
    }

    private static int type2num(String str) {
        int idx = 0;
        for (;idx < types.length; idx++) {
            if (types[idx].equals(str)) return idx;
        }
        return 0;
    }
}

