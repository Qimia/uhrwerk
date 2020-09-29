package io.qimia.uhrwerk.example.TPCDI

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{TableTransformation, TaskInput, TaskOutput}

class ClassJoiner extends TableTransformation {
  /**
   * Code for the transformation required to produce a particular Table
   * (UserCode)
   *
   * @param in TaskInput, all data needed by a user
   * @return TaskOutput which is a dataframe and info about writing the dataframe
   */
  override def process(in: TaskInput): TaskOutput = {

    val Fact_Trade = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("Fact_Trade")).get._2
    val Accounts_unpartitioned = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("Accounts_unpartitioned")).get._2
    val DIM_StatusType = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("DIM_StatusType")).get._2
    val DIM_TradeType = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("DIM_TradeType")).get._2

    TaskOutput(
      Fact_Trade
        .join(Accounts_unpartitioned,Fact_Trade("T_CA_ID") === Accounts_unpartitioned("_CA_ID"), "inner")
        .join(DIM_StatusType,Fact_Trade("T_ST_ID") === DIM_StatusType("ST_ID"), "inner")
        .join(DIM_TradeType,Fact_Trade("T_TT_ID") === DIM_TradeType("TT_ID"), "inner")
    )

  }
}
