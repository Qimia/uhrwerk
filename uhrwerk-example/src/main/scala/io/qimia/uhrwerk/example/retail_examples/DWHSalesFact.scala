package io.qimia.uhrwerk.example.retail_examples

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{TableTransformation, TaskInput, TaskOutput}
import org.apache.spark.sql.functions.col

class DWHSalesFact extends TableTransformation {
  /**
   * Code for the transformation required to produce a particular Table
   * (UserCode)
   *
   * @param in TaskInput, all data needed by a user
   * @return TaskOutput which is a dataframe and info about writing the dataframe
   */
  override def process(in: TaskInput): TaskOutput = {
    val facts = in.loadedInputFrames.get(TableIdent("staging", "retail", "salesFacts", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table salesFact not found!")
    }
    val employeeDim = in.loadedInputFrames.get(TableIdent("staging", "retail", "employeeDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table employeeDim not found!")
    }
    val storeDim = in.loadedInputFrames.get(TableIdent("staging", "retail", "storeDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table storeDim not found!")
    }
    val productDim = in.loadedInputFrames.get(TableIdent("staging", "retail", "productDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table productDim not found!")
    }

    val pFacts = facts.as("f").join(productDim.as("p"), col("f.product_id") === col("p.product_id"))
      .select("f.product_id", "f.quantity", "f.sales_id", "f.cashier", "f.store", "f.selling_date", "f.year",
        "f.month", "f.day", "p.productsKey")

    val sFacts = pFacts.as("p").join(storeDim.as("s"), col("p.store") === col("s.store_id"))
      .select("p.product_id", "p.quantity", "p.sales_id", "p.cashier", "p.store", "p.selling_date", "p.year",
        "p.month", "p.day", "p.productsKey", "s.storesKey")

    val eFacts = sFacts.as("s").join(employeeDim.as("e"), col("s.cashier") === col("e.employee_id"))
      .select("s.product_id", "s.quantity", "s.sales_id", "s.store", "s.selling_date", "s.year",
        "s.month", "s.day", "s.productsKey", "s.storesKey", "e.employeesKey")

    TaskOutput(eFacts)
  }
}
