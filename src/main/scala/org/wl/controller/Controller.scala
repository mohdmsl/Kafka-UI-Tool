package org.wl.controller

import javafx.stage.Stage
import org.wl.view.HomeView

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
