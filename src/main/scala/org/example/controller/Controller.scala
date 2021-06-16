package org.example.controller

import javafx.stage.Stage
import org.example.view.HomeView

object Controller {

  def startApp(stage: Stage): Unit = {
    new HomeView().createView(stage)
  }

  def closeApp(stage: Stage): Unit = {
    stage.close()
  }

  import javafx.fxml.FXML


  @FXML
  def initialize(): Unit = {
    System.out.println("second")
  }

}
