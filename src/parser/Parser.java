/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parser;

import java.sql.SQLException;




/**
 *
 * @author enghin
 */
public class Parser {
    
    private static final String[] filePath={ "/home/enghin/Documents/ISIS Data/#AllEyesOnISIS.json",
                                             "/home/enghin/Documents/ISIS Data/#AllEyesOnISIS-2014-06-20T14385.json",
                                             "/home/enghin/Documents/ISIS Data/#AllEyesOnISIS-2014-06-21T03384.json",
                                             "/home/enghin/Documents/ISIS Data/#CalamityWillBefallUS.json"
                                             };

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws SQLException {
       
        Controller myController = new Controller();
        //myController.readFile(filePath[0]);
        //myController.parseData();
        //myController.insertTweet("من نحن :)", " من نحن", "من نحن  ", "من نحن", "من نحن", "من نحن", 1, "من نحن");
        myController.tryHash();
        
         //myTweet.readFile(filePath[3]);
        System.out.println("من نحن".replaceAll("[\uD83C-\uDBFF\uDC00-\uDFFF]+", ""));
        
    }
    
}
