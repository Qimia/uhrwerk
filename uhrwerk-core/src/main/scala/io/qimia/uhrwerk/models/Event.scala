package io.qimia.uhrwerk.models

import java.util.Date

import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.GeneratedValue
import javax.persistence.Id
import javax.persistence.Table
import javax.persistence.Temporal
import javax.persistence.TemporalType

import org.hibernate.annotations.GenericGenerator

@Entity
@Table(name = "EVENTS") class Event() // this form used by Hibernate
{
  var id = 0L
  var title: Option[String] = _
  var date: Date = _

  def this(title: Option[String], date: Date) {
    this()
    // for application use, to create new events
    this.title = title
    this.date = date
  }

  @Id
  @GeneratedValue(generator = "increment")
  @GenericGenerator(name = "increment", strategy = "increment") def getId: Long = id

  private def setId(id: Long): Unit = {
    this.id = id
  }

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "EVENT_DATE") def getDate: Date = date

  def setDate(date: Date): Unit = {
    this.date = date
  }

  def getTitle: Option[String] = title

  def setTitle(title: Option[String]): Unit = {
    this.title = title
  }
}