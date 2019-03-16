package playground.api

import playground.api.Container.DatasetName

object DatasetNames {
  object Explode {
    object FootballMatchCompleteDatasetNames {
      case object HomeDf extends DatasetName
      case object AwayDf extends DatasetName
      case object Res1 extends DatasetName
    }

    object EplStandingReceiveDatasetNames {
      case object Res2 extends DatasetName
    }
  }
}
