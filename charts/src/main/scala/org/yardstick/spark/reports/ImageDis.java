package org.yardstick.spark.reports;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

/**
 * Created by sany on 7/9/15.
 */
public  class ImageDis extends Application {
    Image labelImage = new Image("file://home/sany/Pictures/GenerateData.png");
    public static void main(String[] args) {
        launchImagePane();
    }
    public static void launchImagePane(){
        Application.launch();
    }
    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Load Image");

        StackPane sp = new StackPane();
        Image img = new Image("file:///home/sany/Pictures/GenerateData.png");
        ImageView imgView = new ImageView(img);
        sp.getChildren().add(imgView);

        //Adding HBox to the scene
        Scene scene = new Scene(sp);
        primaryStage.setScene(scene);
        primaryStage.show();
    }
    //public static void main(String args[]){
    //    ImageDis im=new ImageDis();
//        im.ImageDis();
   // }
}
