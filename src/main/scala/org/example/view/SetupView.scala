package org.example.view

import javafx.geometry.{Insets, Pos}
import javafx.scene.Scene
import javafx.scene.control.{Button, Label, TextField}
import javafx.scene.layout._
import javafx.stage.Stage
import org.example.utility.TopicUtils

import java.util.Properties
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class SetupView {

  def createSetupView(stage: Stage): Unit = {
    val window = new Stage()
    window.setTitle("KAFKA UI TOOL")
    val layout: GridPane = getGridPane
    val setupScene = new Scene(layout, 640, 480)
    window.setScene(setupScene)
    setupScene.getStylesheets.add("/styles/Styles.css")
    window.setOnCloseRequest(e => {
      e.consume()
      closeStage(window)
    })

    //bootstrap label
    val boostrapLabel = new Label("BootStrap Servers")
    layout.add(boostrapLabel, 0 ,0)

    //bootstrap field
    val bootStrap = new TextField()
    bootStrap.setPromptText("Bootstrap Servers")
    layout.add(bootStrap, 1, 0)


    //connect
    val connectBtn = new Button("Connect")
    connectBtn.setOnMouseClicked(event => {
      System.out.println("list topics")
      val bootstrapServers = bootStrap.getText
      val config = new Properties()
      config.put("bootstrap.servers", bootstrapServers)
      TopicUtils.getAllTopics(config).foreach(println(_))
    })

    layout.add(connectBtn,1,1)
    connectBtn.setAlignment(Pos.CENTER)
    window.show()
  }

  def closeStage(stage: Stage): Unit = {
    System.out.println("closed")
    stage.close()
  }

  private def getGridPane = {
    val pane: GridPane = new GridPane()
    pane.setPadding(new Insets(10, 10, 10, 10))
    pane.setHgap(10)
    pane.setVgap(8)
    pane
  }
}
