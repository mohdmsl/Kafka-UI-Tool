package org.wl.view

import javafx.scene.Scene
import javafx.scene.control.Button
import javafx.scene.layout.StackPane
import javafx.stage.Stage

class HomeView {


  def createView(stage: Stage): Unit = {
    stage.setTitle("KAFKA UI TOOL")
    val layout = new StackPane
    val homeScene = new Scene(layout, 640, 480)
    stage.setScene(homeScene)
    val settingsButton = new Button
    settingsButton.setText("settings")
    val setupView = new SetupView
    settingsButton.setOnAction(e => {
       setupView.createSetupView(stage)
    })
    homeScene.getStylesheets.add("/styles/Styles.css")
    layout.getChildren.addAll(settingsButton)
    stage.show()
  }

}
