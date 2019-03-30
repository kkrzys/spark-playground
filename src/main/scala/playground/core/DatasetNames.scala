package playground.core

import playground.core.DataContainer.DatasetName

object DatasetNames {

  object Ingest {
    object FootballMatchCompleteDatasetNames {
      case object ResultFootballMatchCompleteDf extends DatasetName
    }
    object EplStandingReceiveDatasetNames {
      case object ResultEplStandingReceiveDf extends DatasetName
    }
  }

  object Distribute {
    object FootballMatchCompleteDatasetNames {
      case object ResultFootballMatchCompleteDf extends DatasetName
    }
    object EplStandingReceiveDatasetNames {
      case object ResultEplStandingReceiveDf extends DatasetName
    }
  }

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
