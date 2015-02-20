/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author enghin
 */
public class Controller {

    Connection con = null;
    Iterator iterator;
    JSONArray tweetsArray;
    JSONObject innerObj;
    PrintWriter writer;
    SimpleDateFormat formatter;

    public Controller()  {
        
        try {
            writer = new PrintWriter("/home/enghin/Documents/ISIS-data/TweetingGraph.txt", "UTF-8");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null, ex);
        }


        String url = "jdbc:mysql://localhost:3306/DataSet";
        String utf = "?useUnicode=yes&characterEncoding=UTF-8";
        String user = "root";
        String password = "";
        PreparedStatement preparedStatement = null;
        
        formatter = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy");

        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("MySQL JDBC Driver Registered!");
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver not found !!");
            return;
        }

        try {
            con = DriverManager.getConnection(url + utf, user, password);
            System.out.println("SQL Connection to database established!");

        } catch (SQLException ex) {

        }
    }

    public void readFile(String filePath) {

        try {
            FileReader reader = new FileReader(filePath);

            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);

            // get an array from the JSON object
            this.tweetsArray = (JSONArray) jsonObject.get("statuses");
            this.iterator = tweetsArray.iterator();
        } catch (IOException | ParseException e) {
            System.out.println("file not found");
        }
    }

    public void insertTweet(String id, String created_at, String text, String in_reply_status_id,
            String in_reply_user_id, String in_reply_screen_name, int is_retweet,
            String user_id) {

        try {
            String insertTableSQL = "INSERT INTO Tweet"
                    + "(id, created_at, text, in_reply_status_id, in_reply_user_id,"
                    + "in_reply_screenName, is_retweet, user_id) VALUES"
                    + "(?,?,?,?,?,?,?,?)";
            PreparedStatement preparedStatement = con.prepareStatement(insertTableSQL);
            preparedStatement.setString(1, id);
            preparedStatement.setString(2, created_at);
            preparedStatement.setString(3, text);
            preparedStatement.setString(4, in_reply_status_id);
            preparedStatement.setString(5, in_reply_user_id);
            preparedStatement.setString(6, in_reply_screen_name);
            preparedStatement.setInt(7, is_retweet);
            preparedStatement.setString(8, user_id);
            // execute insert SQL stetement
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void insertUser(String user_id, String name, String screen_name, String location,
            String description, String time_zone, String lang) {

        try {
            String insertTableSQL = "INSERT INTO User"
                    + "(user_id, name, screen_name, location, description,"
                    + "time_zone, lang) VALUES"
                    + "(?,?,?,?,?,?,?)";
            PreparedStatement preparedStatement = con.prepareStatement(insertTableSQL);
            preparedStatement.setString(1, user_id);
            preparedStatement.setString(2, name);
            preparedStatement.setString(3, screen_name);
            preparedStatement.setString(4, location);
            preparedStatement.setString(5, description);
            preparedStatement.setString(6, time_zone);
            preparedStatement.setString(7, lang);
            // execute insert SQL stetement
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void parseData() {

        String text = null;
        String id = null;
        String created_at = null;
        String in_reply_user_id = null;
        String in_reply_status_id = null;
        String in_reply_ScreenName = null;
        String user_id = null;
        String name = null;
        String screen_name = null;
        String location = null;
        String description = null;
        String lang = null;
        String time_zone = null;
        int is_retweet = 0;
        int not = 0;
        String retweet = null;

        while (iterator.hasNext()) {//go through tweets
            innerObj = (JSONObject) iterator.next();
            //get text
            text = innerObj.get("text").toString();
            if (text != null) {
                text = text.replaceAll("[\uD83C-\uDBFF\uDC00-\uDFFF]+", "");
            }
            //get id
            id = innerObj.get("id").toString();
            //get created_at
            created_at = innerObj.get("created_at").toString();
            try {
                //get in_reply_user_id
                in_reply_user_id = innerObj.get("in_reply_to_user_id").toString();
            } catch (Exception e) {
            }
            //get in_reply_status_id
            try {
                in_reply_status_id = innerObj.get("in_reply_to_status_id").toString();
            } catch (Exception e) {
            }
            //get in_reply_ScreenName
            try {
                in_reply_ScreenName = innerObj.get("in_reply_to_screen_name").toString();
            } catch (Exception e) {
            }

            try {
                retweet = innerObj.get("retweeted_status").toString();
                is_retweet = 1;
            } catch (Exception e) {
                is_retweet = 0;
            }

            //go through the user tag
            String userA = innerObj.get("user").toString();
            userA = userA.replaceAll("\"", "");
            //System.out.println(userA);
            String[] user = userA.split(",");

            // String[] user = innerObj.get("user").toString().split(",");
            for (String token : user) {
                String[] partToken = token.split(":");
                        //System.out.println("Test" + partToken[0]);

                if (partToken[0].equals("id_str")) {
                    //get user id
                    user_id = partToken[1];
                    //System.out.println(user_id);
                }
                if (partToken[0].equals("name") && partToken.length == 2) {
                    //get name
                    name = partToken[1];
                                        //System.out.println(name);

                }
                if (partToken[0].equals("screen_name") && partToken.length == 2) {
                    //get screen_name
                    screen_name = partToken[1];

                }
                if (partToken[0].equals("location") && partToken.length == 2) {
                    //get location
                    location = partToken[1];
                    //System.out.println(location);
                }
                if (partToken[0].equals("description") && partToken.length == 2) {
                    //get description
                    description = partToken[1];
                    //System.out.println(description);
                }
                if (partToken[0].equals("time_zone") && partToken.length == 2) {
                    //get time_zone
                    time_zone = partToken[1];
                    //System.out.println(time_zone);
                }
                if (partToken[0].equals("lang") && partToken.length == 2) {
                    //get language
                    lang = partToken[1];
                    //System.out.println(lang);
                }

            }
                    //insertTweet(id, created_at, text, in_reply_status_id, in_reply_user_id, in_reply_ScreenName, is_retweet, user_id);
            //insertUser(user_id, name, screen_name, location, description, time_zone, lang);
        }
    }

    public void selectRetweets() {

        int count = 0;
        String sql = "SELECT distinct text FROM tbl_Tweet WHERE is_retweet = 1";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            ResultSet rs = preparedStatement.executeQuery(sql);

            while (rs.next()) {
                writer.println("**************************");
                System.out.println(""+count);
                String text = rs.getString("text");
                //System.out.println(" " + text);
                getRetweets(text);
                count++;
            }
            //writer.close();
        } catch (SQLException ex) {
            Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void getRetweets(String text) throws SQLException {

        ArrayList<Long> seconds = new ArrayList<>();
        Statement st = con.createStatement();
        //System.out.println(text);
        writer.println(text);
        text = text.replaceAll("\'", "%");
       // String sql = ("Select text FROM tbl_Tweet WHERE is_retweet = 1 AND text LIKE  \'%" + ": "+text + "\'");
        String sql = ("Select text, created_at FROM tbl_Tweet WHERE is_retweet = 1 AND text =  \'" +text + "\'");
        //System.out.println(sql);
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()) {
            String reText = rs.getString("text");
            String createdAt = rs.getString("created_at");
            //System.out.println(createdAt);
            writer.println(createdAt);
            try {
                Calendar cal = Calendar.getInstance();
                //Date date = formatter.parse(createdAt);
                cal.setTime(formatter.parse(createdAt));
                seconds.add(cal.getTimeInMillis()/1000);
            } catch (java.text.ParseException ex) {
                Logger.getLogger(Controller.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if(!seconds.isEmpty()){
        compute1hAgingFactor(seconds);
        seconds.clear();
        }
    }
    
    public double compute1hAgingFactor(ArrayList seconds){
    
        double l = 0.0;
        double k = 0.0;
        long secArray [] = new long [seconds.size()];
        Collections.sort(seconds);
        long min = Long.parseLong(seconds.get(0).toString());
        long border = min + 3600;
        for (int i=0; i<seconds.size();i++){
            secArray[i] = Long.parseLong(seconds.get(i).toString());
            if(secArray[i] <= border)
                l++;
            else
                k++;
        }
        double a = k/(k+l);
        //System.out.println("k= "+ k);
        //System.out.println("l= "+ l);
        //System.out.println("A= "+ a);
        writer.println("1hAgingFactor = "+a);
        return k/(k+l);
    }
    
    public void getCommonUsers() throws SQLException{
    
        int count = 0;
        Statement st = con.createStatement();
        String sql = ("Select Id  FROM tbl_twitter_ISIS_Tweet ");
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()) {
            String Id = rs.getString("Id");
            System.out.println(" " + count );
            
             Statement st1 = con.createStatement();
             String sql1 = ("Select distinct user_id , screen_name  FROM User WHERE screen_name =  \'" +Id + "\' ");
             ResultSet rs1 = st1.executeQuery(sql1);
                while(rs1.next()){
                
                    String screen_name = rs1.getString("screen_name");
                    String user_id = rs1.getString("user_id");
                    
                    Statement st2 = con.createStatement();
                    String sql2 = ("UPDATE tbl_twitter_ISIS_Tweet SET user_id =\'" +user_id + "\' where Id = \'" +screen_name + "\'");
                    st2.executeUpdate(sql2);
                }
            
            count++;
        }
    }
    
    public void tryHash() throws SQLException{
    
        int count = 0;
        String userName = "";
        String user_id = "";
        HashMap<String, List<String>> map = new HashMap<String, List<String> >();
        Statement st = con.createStatement();
        String sql = ("Select text, user_id  FROM tbl_Tweet where is_retweet = 1");
        ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                System.out.println(count);
                count++;
                String text = rs.getString("text");
                user_id = rs.getString("user_id");
                String[] partToken = text.split(" ");
                userName = partToken[1].substring(1, partToken[1].length()-1);
                
                List<String> auxArray = new ArrayList<String>();
                
                if(map.containsKey(userName)){

                    auxArray = map.get(userName);
                    auxArray.add(user_id);
                    map.put(userName, auxArray);
                }else{

                    auxArray.add(user_id);
                    map.put(userName, auxArray);
                }
            }
        
        
 
        // iterate and display values
        System.out.println("Fetching Keys and corresponding [Multiple] Values n");
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            writer.println("Key = " + key);
            writer.println("Values = " + values);
            System.out.println("Key = " + key);
            System.out.println("Values = " + values );
        }
    }

}
