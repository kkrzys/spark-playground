package playground.api

import playground.api.Container.DatasetName

sealed trait DatasetNames

object FootballMatchCompleteExplodeDatasetNames extends DatasetNames {
  case object HomeDf extends DatasetName
  case object AwayDf extends DatasetName
  case object Res1 extends DatasetName
}

object EplStandingReceiveExplodeDatasetNames extends DatasetNames {
  case object Res2 extends DatasetName
}
